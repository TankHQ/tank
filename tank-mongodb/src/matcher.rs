use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::Document;
use std::mem;
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
        let l_matcher = &mut IsColumn::default();
        let r_matcher = &mut IsColumn::default();
        let result = matches!(
            op,
            BinaryOpType::In
                | BinaryOpType::NotIn
                | BinaryOpType::Equal
                | BinaryOpType::NotEqual
                | BinaryOpType::Less
                | BinaryOpType::Greater
                | BinaryOpType::LessEqual
                | BinaryOpType::GreaterEqual
        ) && (lhs.matches(l_matcher) != rhs.matches(r_matcher));
        if !result {
            return false;
        }
        let (field, value, op) = if let Some(field) = mem::take(&mut l_matcher.column) {
            (field, rhs, *op)
        } else if let Some(field) = mem::take(&mut r_matcher.column) {
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
