use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::Document;
use std::{borrow::Cow, mem};
use tank_core::{BinaryOpType, ColumnRef, Expression, ExpressionMatcher, Operand};

#[derive(Default)]
pub struct IsColumn {
    pub column: Option<ColumnRef>,
}
impl ExpressionMatcher for IsColumn {
    fn match_column(&mut self, column: &ColumnRef) -> bool {
        self.column = Some(column.clone());
        true
    }
    fn match_operand(&mut self, operand: &Operand) -> bool {
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

#[derive(Default)]
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
        op: &BinaryOpType,
        lhs: &dyn Expression,
        rhs: &dyn Expression,
    ) -> bool {
        if *op == BinaryOpType::And {
            let mut left = IsFieldCondition::default();
            let mut right = IsFieldCondition::default();
            if lhs.matches(&mut left) && rhs.matches(&mut right) {
                self.condition.extend(left.condition);
                self.condition.extend(right.condition);
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
        let lhs_is_col = lhs.matches(&mut l_column);
        let rhs_is_col = rhs.matches(&mut r_column);
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
        let op = writer.expression_binary_op_key(op);
        self.condition
            .insert(field.name, Document::from_iter([(op.into(), fragment)]));
        true
    }
}
