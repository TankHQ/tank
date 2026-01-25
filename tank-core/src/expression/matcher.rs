use crate::{BinaryOpType, ColumnRef, Expression, Operand, Ordered, UnaryOpType, Value};

pub trait ExpressionMatcher {
    fn match_column(&mut self, _column: &ColumnRef) -> bool {
        false
    }
    fn match_operand(&mut self, _operand: &Operand) -> bool {
        false
    }
    fn match_unary_op(&mut self, _ty: &UnaryOpType, _arg: &dyn Expression) -> bool {
        false
    }
    fn match_binary_op(
        &mut self,
        _ty: &BinaryOpType,
        _lhs: &dyn Expression,
        _rhs: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_ordered(&mut self, _ordered: &Ordered<&dyn Expression>) -> bool {
        false
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsOrdered;
impl ExpressionMatcher for IsOrdered {
    fn match_ordered(&mut self, _ordered: &Ordered<&dyn Expression>) -> bool {
        true
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsTrue;
impl ExpressionMatcher for IsTrue {
    fn match_operand(&mut self, operand: &Operand) -> bool {
        match operand {
            Operand::LitBool(v) => *v == true,
            Operand::Variable(Value::Boolean(Some(true), ..)) => true,
            Operand::Value(Value::Boolean(Some(true), ..)) => true,
            _ => false,
        }
    }
}
