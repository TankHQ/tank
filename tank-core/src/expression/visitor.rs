use crate::{
    BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, Operand, Order, Ordered,
    SqlWriter, UnaryOp, Value,
};

pub trait ExpressionVisitor {
    fn visit_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &ColumnRef,
    ) -> bool {
        false
    }
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &Operand,
    ) -> bool {
        false
    }
    fn visit_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &UnaryOp<&dyn Expression>,
    ) -> bool {
        false
    }
    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        false
    }
    fn visit_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &Ordered<&dyn Expression>,
    ) -> bool {
        false
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FindOrder {
    pub order: Order,
}
impl ExpressionVisitor for FindOrder {
    fn visit_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &ColumnRef,
    ) -> bool {
        true
    }
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &Operand,
    ) -> bool {
        true
    }
    fn visit_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &UnaryOp<&dyn Expression>,
    ) -> bool {
        true
    }
    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        true
    }
    fn visit_ordered(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        self.order = value.order;
        true
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsTrue;
impl ExpressionVisitor for IsTrue {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitBool(true)
            | Operand::Variable(Value::Boolean(Some(true), ..))
            | Operand::Value(Value::Boolean(Some(true), ..)) => true,
            _ => false,
        }
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct IsFalse;
impl ExpressionVisitor for IsFalse {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitBool(false)
            | Operand::Variable(Value::Boolean(Some(false), ..))
            | Operand::Value(Value::Boolean(Some(false), ..)) => true,
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct IsAggregateFunction;
impl ExpressionVisitor for IsAggregateFunction {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
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
    fn visit_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        if value.op == BinaryOpType::Alias {
            value.lhs.accept_visitor(self, writer, context, out)
        } else {
            false
        }
    }
}

#[derive(Default, Debug)]
pub struct IsAsterisk;
impl ExpressionVisitor for IsAsterisk {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        matches!(value, Operand::Asterisk)
    }
}

#[derive(Default, Debug)]
pub struct IsQuestionMark;
impl ExpressionVisitor for IsQuestionMark {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        matches!(value, Operand::QuestionMark)
    }
}

#[derive(Default, Debug)]
pub struct IsAlias {
    pub name: String,
}
impl ExpressionVisitor for IsAlias {
    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        if value.op != BinaryOpType::Alias {
            return false;
        }
        self.name = value.rhs.as_identifier(context);
        true
    }
}
