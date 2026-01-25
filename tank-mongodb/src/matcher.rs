use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::Document;
use std::mem;
use tank_core::{BinaryOpType, ColumnRef, Expression, ExpressionMatcher};

#[derive(Default)]
pub struct ColumnMatcher {
    pub column: Option<ColumnRef>,
}
impl ExpressionMatcher for ColumnMatcher {
    fn match_column(&mut self, column: &ColumnRef) -> bool {
        self.column = Some(column.clone());
        true
    }
}

#[derive(Default)]
pub struct FieldConditionMatcher {
    pub field_condition: Document,
}
impl ExpressionMatcher for FieldConditionMatcher {
    fn match_binary_op(
        &mut self,
        op: &BinaryOpType,
        lhs: &dyn Expression,
        rhs: &dyn Expression,
    ) -> bool {
        let l_matcher = &mut ColumnMatcher::default();
        let r_matcher = &mut ColumnMatcher::default();
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
        let (field, value, op) = if let Some(field) = l_matcher.column {
            (field, rhs, *op)
        } else if let Some(field) = r_matcher.column {
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
        self.field_condition
            .insert(field.name, Document::from_iter([(op.into(), fragment)]));
        true
    }
}
