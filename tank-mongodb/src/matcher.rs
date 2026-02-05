use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::{self, Bson, Document, doc};
use std::{borrow::Cow, mem};
use tank_core::{
    BinaryOpType, Context, Expression, ExpressionMatcher, IsAsterisk, IsColumn, Operand, Ordered,
    SqlWriter, UnaryOpType,
};

#[derive(Default, Debug)]
pub struct IsConstant;
impl ExpressionMatcher for IsConstant {
    fn match_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        operand: &Operand,
    ) -> bool {
        match operand {
            Operand::Null
            | Operand::LitBool(..)
            | Operand::LitInt(..)
            | Operand::LitFloat(..)
            | Operand::LitStr(..)
            | Operand::Type(..)
            | Operand::Variable(..)
            | Operand::Value(..)
            | Operand::Asterisk => true,
            Operand::LitIdent(..) | Operand::LitField(..) | Operand::QuestionMark => false,
            Operand::LitArray(operands) | Operand::LitTuple(operands) => operands
                .iter()
                .all(|v| v.matches(&mut IsConstant, writer, context)),
            Operand::Call(_, expressions) => expressions
                .iter()
                .all(|v| v.matches(&mut IsConstant, writer, context)),
        }
    }

    fn match_unary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        _ty: &UnaryOpType,
        arg: &dyn Expression,
    ) -> bool {
        arg.matches(self, writer, context)
    }

    fn match_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        ty: &BinaryOpType,
        lhs: &dyn Expression,
        rhs: &dyn Expression,
    ) -> bool {
        lhs.matches(self, writer, context) && rhs.matches(self, writer, context)
    }

    fn match_ordered(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        ordered: &Ordered<&dyn Expression>,
    ) -> bool {
        ordered.expression.matches(self, writer, context)
    }
}

#[derive(Default, Debug)]
pub struct IsFieldCondition {
    pub table: Cow<'static, str>,
    pub condition: Document,
}
impl IsFieldCondition {
    pub fn new() -> Self {
        IsFieldCondition::default()
    }
    pub fn with_table(table: Cow<'static, str>) -> Self {
        IsFieldCondition {
            table,
            condition: Default::default(),
        }
    }
}
impl ExpressionMatcher for IsFieldCondition {
    fn match_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        op: &BinaryOpType,
        lhs: &dyn Expression,
        rhs: &dyn Expression,
    ) -> bool {
        if matches!(*op, BinaryOpType::And | BinaryOpType::Or) {
            let mut left = IsFieldCondition::default();
            let mut right = IsFieldCondition::default();
            let mut c = context.clone();
            let left_matches = lhs.matches(&mut left, writer, &mut c);
            let right_matches = rhs.matches(&mut right, writer, &mut c);
            if left_matches || right_matches {
                let op = MongoDBSqlWriter::default()
                    .expression_binary_op_key(*op)
                    .to_string();
                macro_rules! write_query {
                    ($matcher:ident) => {
                        if !$matcher.condition.is_empty() {
                            $matcher.condition.into()
                        } else {
                            let mut query = MongoDBSqlWriter::make_prepared();
                            lhs.write_query(writer, &mut c, &mut query);
                            let Some(bson) = query
                                .as_prepared::<MongoDBDriver>()
                                .and_then(MongoDBPrepared::current_bson)
                            else {
                                log::error!("Failed to get the bson object from write_query");
                                return false;
                            };
                            mem::take(bson)
                        }
                    };
                }
                let mut left = write_query!(left);
                let mut right = write_query!(right);
                let mut args = bson::Array::new();
                if let Some(left) = left.as_document_mut()
                    && left.len() == 1
                    && let Ok(left) = left.get_array_mut(&op)
                {
                    args.append(left);
                } else {
                    args.push(left);
                };
                if let Some(right) = right.as_document_mut()
                    && right.len() == 1
                    && let Ok(right) = right.get_array_mut(&op)
                {
                    args.append(right);
                } else {
                    args.push(right);
                };
                self.condition = doc! { op: Bson::Array(args) };
                *context = c;
                return true;
            }
        }
        if !matches!(
            op,
            BinaryOpType::In
                | BinaryOpType::NotIn
                | BinaryOpType::Is
                | BinaryOpType::IsNot
                | BinaryOpType::Equal
                | BinaryOpType::NotEqual
                | BinaryOpType::Less
                | BinaryOpType::Greater
                | BinaryOpType::LessEqual
                | BinaryOpType::GreaterEqual
        ) {
            return false;
        }
        let mut c = context.clone();
        let mut l_column = IsColumn::default();
        let mut r_column = IsColumn::default();
        let lhs_is_col = lhs.matches(&mut l_column, writer, &mut c);
        let rhs_is_col = rhs.matches(&mut r_column, writer, &mut c);
        if lhs_is_col == rhs_is_col {
            return false;
        }
        let (field, value, op) = if let Some(field) = l_column.column {
            (field, rhs, *op)
        } else if let Some(field) = mem::take(&mut r_column.column) {
            (
                field,
                lhs,
                match op {
                    BinaryOpType::Less => BinaryOpType::Greater,
                    BinaryOpType::Greater => BinaryOpType::Less,
                    BinaryOpType::LessEqual => BinaryOpType::GreaterEqual,
                    BinaryOpType::GreaterEqual => BinaryOpType::LessEqual,
                    _ => *op,
                },
            )
        } else {
            // Unreachable
            log::error!(
                "Unexpected error, the matcher conditions succeeded but the field was not found"
            );
            return false;
        };
        if !value.matches(&mut IsConstant, writer, context) {
            return false;
        }
        let writer = MongoDBSqlWriter {};
        let mut fragment = MongoDBSqlWriter::make_prepared();
        value.write_query(&writer, &mut c, &mut fragment);
        let Some(fragment) = fragment
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
            .map(mem::take)
        else {
            // Unreachable
            log::error!(
                "Unexpected error, for some reason the rendered value does not have a current bson"
            );
            return false;
        };
        let mut name = field.name;
        if !self.table.is_empty() {
            name = format!("{}.{}", self.table, name).into();
        }
        self.condition.insert(
            name,
            if op == BinaryOpType::Equal {
                fragment
            } else {
                let op = writer.expression_binary_op_key(op).to_string();
                doc! { op: fragment }.into()
            },
        );
        *context = c;
        true
    }
}

#[derive(Default, Debug)]
pub struct IsCount;
impl ExpressionMatcher for IsCount {
    fn match_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        operand: &Operand,
    ) -> bool {
        match operand {
            Operand::Call(function, args) => {
                if function.eq_ignore_ascii_case("count")
                    && let [arg] = args
                    && let mut c = context.clone()
                    && arg.matches(&mut IsAsterisk, writer, &mut c)
                {
                    *context = c;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}
