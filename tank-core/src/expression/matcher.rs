use crate::{
    BinaryOpType, ColumnRef, Context, Expression, Operand, Order, Ordered, SqlWriter, UnaryOpType,
    Value,
};
use std::borrow::Cow;

pub trait ExpressionMatcher {
    fn match_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _column: &ColumnRef,
    ) -> bool {
        false
    }
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _operand: &Operand,
    ) -> bool {
        false
    }
    fn match_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _ty: &UnaryOpType,
        _arg: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _ty: &BinaryOpType,
        _lhs: &dyn Expression,
        _rhs: &dyn Expression,
    ) -> bool {
        false
    }
    fn match_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _ordered: &Ordered<&dyn Expression>,
    ) -> bool {
        false
    }
}

#[derive(Default, Debug)]
pub struct IsColumn {
    pub column: Option<ColumnRef>,
}
impl ExpressionMatcher for IsColumn {
    fn match_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        column: &ColumnRef,
    ) -> bool {
        self.column = Some(column.clone());
        true
    }
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
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

#[derive(Default, Debug, Copy, Clone)]
pub struct FindOrder {
    pub order: Order,
}
impl ExpressionMatcher for FindOrder {
    fn match_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _column: &ColumnRef,
    ) -> bool {
        true
    }
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _operand: &Operand,
    ) -> bool {
        true
    }
    fn match_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _ty: &UnaryOpType,
        _arg: &dyn Expression,
    ) -> bool {
        true
    }
    fn match_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _ty: &BinaryOpType,
        _lhs: &dyn Expression,
        _rhs: &dyn Expression,
    ) -> bool {
        true
    }
    fn match_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        ordered: &Ordered<&dyn Expression>,
    ) -> bool {
        self.order = ordered.order;
        true
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsTrue;
impl ExpressionMatcher for IsTrue {
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
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
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
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
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
        match operand {
            Operand::Call(function, ..) => match function {
                s if s.eq_ignore_ascii_case("abs") => true,
                s if s.eq_ignore_ascii_case("avg") => true,
                s if s.eq_ignore_ascii_case("count") => true,
                s if s.eq_ignore_ascii_case("max") => true,
                s if s.eq_ignore_ascii_case("min") => true,
                s if s.eq_ignore_ascii_case("sum") => true,
                _ => false,
            },
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct IsAsterisk;
impl ExpressionMatcher for IsAsterisk {
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
        matches!(operand, Operand::Asterisk)
    }
}

#[derive(Default, Debug)]
pub struct IsQuestionMark;
impl ExpressionMatcher for IsQuestionMark {
    fn match_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        operand: &Operand,
    ) -> bool {
        matches!(operand, Operand::QuestionMark)
    }
}
