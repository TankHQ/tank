use crate::{BinaryOpType, ColumnRef, Expression, Operand, Ordered, UnaryOpType, Value};

pub trait ExpressionMatcher {
    fn match_column(&self, _column: &ColumnRef) -> bool {
        false
    }
    fn match_operand(&self, _operand: &Operand) -> bool {
        false
    }
    fn match_unary_op(&self, _ty: &UnaryOpType, _arg: &dyn Expression) -> bool {
        false
    }
    fn match_binary_op(
        &self,
        _ty: &BinaryOpType,
        _lhs: &dyn Expression,
        _rhs: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_ordered(&self, _ordered: &Ordered<&dyn Expression>) -> bool {
        false
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsOrdered;
impl ExpressionMatcher for IsOrdered {
    fn match_ordered(&self, _ordered: &Ordered<&dyn Expression>) -> bool {
        true
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsTrue;
impl ExpressionMatcher for IsTrue {
    fn match_operand(&self, operand: &Operand) -> bool {
        match operand {
            Operand::LitBool(v) => *v == true,
            Operand::Variable(Value::Boolean(Some(true), ..)) => true,
            Operand::Value(Value::Boolean(Some(true), ..)) => true,
            _ => false,
        }
    }
}
