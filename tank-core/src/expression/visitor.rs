use crate::{
    BinaryOp, BinaryOpType, Case, ColumnRef, Context, DynQuery, Expression, GenericSqlWriter,
    Operand, Order, Ordered, Result, SqlWriter, UnaryOp, Value, ValueOwned,
};
use anyhow::anyhow;
use std::borrow::Cow;

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
    fn visit_case(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &Case,
    ) -> bool {
        false
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

/// Value of a literal expression, for drivers that store native values instead of SQL text.
#[derive(Default)]
pub struct LiteralValue(Option<ValueOwned>);

impl LiteralValue {
    pub fn new(expression: impl Expression) -> Result<ValueOwned> {
        let mut literal = Self::default();
        expression.accept_visitor(
            &mut literal,
            &GenericSqlWriter,
            &mut Context::empty(),
            &mut DynQuery::default(),
        );
        literal
            .0
            .ok_or_else(|| anyhow!("Expected a literal value, found a SQL expression"))
    }
}

impl ExpressionVisitor for LiteralValue {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        self.0 = Some(match value {
            Operand::Null => Value::Null,
            Operand::LitBool(v) => Value::Boolean(Some(*v)),
            Operand::LitInt(v) => Value::Int128(Some(Box::new(*v))),
            Operand::LitFloat(v) => Value::Float64(Some(*v)),
            Operand::LitStr(v) => Value::Varchar(Some(Cow::Owned((*v).into()))),
            Operand::Variable(v) => v.clone().into_owned(),
            Operand::Value(v) => (*v).clone().into_owned(),
            _ => return false,
        });
        true
    }
}

#[derive(Default, Debug)]
pub struct IsConstant;
impl ExpressionVisitor for IsConstant {
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::Null
            | Operand::LitBool(..)
            | Operand::LitInt(..)
            | Operand::LitFloat(..)
            | Operand::LitStr(..)
            | Operand::Type(..)
            | Operand::Variable(..)
            | Operand::Value(..) => true,
            Operand::LitList(operands) | Operand::LitTuple(operands) => operands
                .iter()
                .all(|v| v.accept_visitor(&mut IsConstant, writer, context, out)),
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

/// Renders `IN`/`NOT IN` rhs as SQL tuples `(1,2,3)` (not array literals `[1,2,3]` as they normally would).
#[derive(Default, Debug, Copy, Clone)]
pub struct WriteInList;
impl ExpressionVisitor for WriteInList {
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitList(v) | Operand::LitTuple(v) => {
                writer.write_tuple(context, out, &mut v.iter().copied())
            }
            Operand::Variable(Value::Array(Some(v), ..) | Value::List(Some(v), ..))
            | Operand::Value(Value::Array(Some(v), ..) | Value::List(Some(v), ..)) => {
                writer.write_tuple(context, out, &mut v.iter().map(|v| v as _))
            }
            _ => return false,
        }
        true
    }
}
