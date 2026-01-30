use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::{self, Bson, Document, doc};
use std::{borrow::Cow, mem};
use tank_core::{BinaryOpType, ColumnRef, Expression, ExpressionMatcher, Operand, SqlWriter};

#[derive(Default, Debug)]
pub struct IsColumn {
    pub column: Option<ColumnRef>,
}
impl ExpressionMatcher for IsColumn {
    fn match_column(&mut self, _writer: &dyn SqlWriter, column: &ColumnRef) -> bool {
        self.column = Some(column.clone());
        true
    }
    fn match_operand(&mut self, _writer: &dyn SqlWriter, operand: &Operand) -> bool {
        match operand {
            Operand::LitIdent(v) => {
                self.column = Some(ColumnRef {
                    name: Cow::Owned(v.to_string()),
                    table: "".into(),
                    schema: "".into(),
                });
                true
            }
            Operand::LitField(v) => {
                let mut it = v.into_iter().rev();
                let name = it.next().map(ToString::to_string).unwrap_or_default();
                let table = it.next().map(ToString::to_string).unwrap_or_default();
                let schema = it.next().map(ToString::to_string).unwrap_or_default();
                self.column = Some(ColumnRef {
                    name: name.into(),
                    table: table.into(),
                    schema: schema.into(),
                });
                true
            }
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct IsFieldCondition {
    pub condition: Document,
}
impl IsFieldCondition {
    pub fn new() -> Self {
        IsFieldCondition::default()
    }
}
impl ExpressionMatcher for IsFieldCondition {
    fn match_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        op: &BinaryOpType,
        lhs: &dyn Expression,
        rhs: &dyn Expression,
    ) -> bool {
        if matches!(*op, BinaryOpType::And | BinaryOpType::Or) {
            let mut left = IsFieldCondition::default();
            let mut right = IsFieldCondition::default();
            let left_matches = lhs.matches(&mut left, writer);
            let right_matches = rhs.matches(&mut right, writer);
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
                            lhs.write_query(writer, &mut Default::default(), &mut query);
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
        let mut l_column = IsColumn::default();
        let mut r_column = IsColumn::default();
        let lhs_is_col = lhs.matches(&mut l_column, writer);
        let rhs_is_col = rhs.matches(&mut r_column, writer);
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
        let writer = MongoDBSqlWriter {};
        let mut fragment = MongoDBSqlWriter::make_prepared();
        value.write_query(&writer, &mut Default::default(), &mut fragment);
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
        self.condition.insert(
            field.name,
            if op == BinaryOpType::Equal {
                fragment
            } else {
                let op = writer.expression_binary_op_key(op).to_string();
                doc! { op: fragment }.into()
            },
        );
        true
    }
}
