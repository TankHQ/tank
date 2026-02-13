use crate::{
    DynQuery, ExpressionVisitor, GenericSqlWriter, OpPrecedence, Operand, Value,
    writer::{Context, SqlWriter},
};
use std::{fmt::Debug, mem};

/// Renderable SQL expression.
pub trait Expression: OpPrecedence + Send + Sync + Debug {
    /// Serialize the expression into `out` using `writer`.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery);
    /// Check if the matcher matching this expression
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
    ) -> bool;
    /// Converts the given value to a `String` representing the expression
    fn as_identifier(&self, context: &mut Context) -> String {
        let mut out = DynQuery::new(String::new());
        let writer = GenericSqlWriter::new();
        self.write_query(&writer, context, &mut out);
        mem::take(out.buffer())
    }
}

impl<T: Expression> Expression for &T {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        (*self).accept_visitor(matcher, writer, context, out)
    }
    fn as_identifier(&self, context: &mut Context) -> String {
        (*self).as_identifier(context)
    }
}

impl Expression for &dyn Expression {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        (*self).accept_visitor(matcher, writer, context, out)
    }
    fn as_identifier(&self, context: &mut Context) -> String {
        (*self).as_identifier(context)
    }
}

impl Expression for () {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut DynQuery) {}
    fn accept_visitor(
        &self,
        _matcher: &mut dyn ExpressionVisitor,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
    ) -> bool {
        false
    }
}

impl Expression for bool {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_bool(context, out, *self);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_operand(writer, context, out, &Operand::LitBool(*self))
    }
}

impl Expression for &'static str {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_string(context, out, self);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_operand(writer, context, out, &Operand::LitStr(*self))
    }
}

impl Expression for Value {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, self);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_operand(writer, context, out, &Operand::Value(self))
    }
}

impl<'a, T: Expression> From<&'a T> for &'a dyn Expression {
    fn from(value: &'a T) -> Self {
        value as &'a dyn Expression
    }
}
