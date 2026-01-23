use crate::{
    DynQuery, ExpressionMatcher, OpPrecedence, Operand, Value,
    writer::{Context, SqlWriter},
};
use std::fmt::Debug;

/// Renderable SQL expression.
pub trait Expression: OpPrecedence + Send + Sync + Debug {
    /// Serialize the expression into `out` using `writer`.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery);
    /// Check if the matcher matching this expression
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool;
}

impl<T: Expression> Expression for &T {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool {
        (*self).matches(matcher)
    }
}

impl Expression for &dyn Expression {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out);
    }
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool {
        (*self).matches(matcher)
    }
}

impl Expression for () {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut DynQuery) {}
    fn matches(&self, _matcher: &dyn ExpressionMatcher) -> bool {
        false
    }
}

impl Expression for bool {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_bool(context, out, *self);
    }
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool {
        matcher.match_operand(&Operand::LitBool(*self))
    }
}

impl Expression for &'static str {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value_string(context, out, self);
    }
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool {
        matcher.match_operand(&Operand::LitStr(*self))
    }
}

impl Expression for Value {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, self);
    }
    fn matches(&self, matcher: &dyn ExpressionMatcher) -> bool {
        matcher.match_operand(&Operand::Value(self))
    }
}

impl<'a, T: Expression> From<&'a T> for &'a dyn Expression {
    fn from(value: &'a T) -> Self {
        value as &'a dyn Expression
    }
}
