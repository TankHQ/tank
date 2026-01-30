use crate::{
    DynQuery, ExpressionMatcher, GenericSqlWriter, OpPrecedence, Operand, Value,
    writer::{Context, SqlWriter},
};
use std::{fmt::Debug, mem};

/// Renderable SQL expression.
pub trait Expression: OpPrecedence + Send + Sync + Debug {
    /// Serialize the expression into `out` using `writer`.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery);
    /// Check if the matcher matching this expression
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool;
    /// Converts the given value to a `String` representing the expression
    fn as_written(&self) -> String {
        let mut out = DynQuery::new(String::new());
        let writer = GenericSqlWriter::new();
        self.write_query(&writer, &mut Default::default(), &mut out);
        mem::take(out.buffer())
    }
}

impl<T: Expression> Expression for &T {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        (*self).matches(matcher, writer)
    }
}

impl Expression for &dyn Expression {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        (*self).matches(matcher, writer)
    }
}

impl Expression for () {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut DynQuery) {}
    fn matches(&self, _matcher: &mut dyn ExpressionMatcher, _writer: &dyn SqlWriter) -> bool {
        false
    }
}

impl Expression for bool {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_bool(context, out, *self);
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        matcher.match_operand(writer, &Operand::LitBool(*self))
    }
}

impl Expression for &'static str {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_string(context, out, self);
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        matcher.match_operand(writer, &Operand::LitStr(*self))
    }
}

impl Expression for Value {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, self);
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        matcher.match_operand(writer, &Operand::Value(self))
    }
}

impl<'a, T: Expression> From<&'a T> for &'a dyn Expression {
    fn from(value: &'a T) -> Self {
        value as &'a dyn Expression
    }
}
