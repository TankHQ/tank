use crate::{BinaryOpType, ColumnRef, Expression, Operand, Ordered, SqlWriter, UnaryOpType, Value};

pub trait ExpressionMatcher {
    fn match_column(&mut self, _writer: &dyn SqlWriter, _column: &ColumnRef) -> bool {
        false
    }
    fn match_operand(&mut self, _writer: &dyn SqlWriter, _operand: &Operand) -> bool {
        false
    }
    fn match_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _ty: &UnaryOpType,
        _arg: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _ty: &BinaryOpType,
        _lhs: &dyn Expression,
        _rhs: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _ordered: &Ordered<&dyn Expression>,
    ) -> bool {
        false
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsOrdered;
impl ExpressionMatcher for IsOrdered {
    fn match_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _ordered: &Ordered<&dyn Expression>,
    ) -> bool {
        true
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsTrue;
impl ExpressionMatcher for IsTrue {
    fn match_operand(&mut self, _writer: &dyn SqlWriter, operand: &Operand) -> bool {
        match operand {
            Operand::LitBool(true)
            | Operand::Variable(Value::Boolean(Some(true), ..))
            | Operand::Value(Value::Boolean(Some(true), ..)) => true,
            _ => false,
        }
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsFalse;
impl ExpressionMatcher for IsFalse {
    fn match_operand(&mut self, _writer: &dyn SqlWriter, operand: &Operand) -> bool {
        match operand {
            Operand::LitBool(false)
            | Operand::Variable(Value::Boolean(Some(false), ..))
            | Operand::Value(Value::Boolean(Some(false), ..)) => true,
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct IsAggregateFunction;
impl ExpressionMatcher for IsAggregateFunction {
    fn match_operand(&mut self, _writer: &dyn SqlWriter, operand: &Operand) -> bool {
        match operand {
            Operand::Call(function, ..) => match function {
                s if s.eq_ignore_ascii_case("abs") => true,
                s if s.eq_ignore_ascii_case("avg") => true,
                s if s.eq_ignore_ascii_case("count") => true,
                s if s.eq_ignore_ascii_case("max") => true,
                s if s.eq_ignore_ascii_case("min") => true,
                _ => false,
            },
            _ => false,
        }
    }
}
