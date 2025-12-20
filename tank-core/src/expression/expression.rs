use crate::{
    OpPrecedence, Value,
    writer::{Context, SqlWriter},
};
use std::fmt::Debug;

/// Renderable SQL expression.
pub trait Expression: OpPrecedence + Send + Sync + Debug {
    /// Serialize the expression into `out` using `writer`.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String);
    /// True if it encodes ordering.
    fn is_ordered(&self) -> bool {
        false
    }
    /// True if it is a expression that simply evaluates to true
    fn is_true(&self) -> bool {
        false
    }
}

impl<T: Expression> Expression for &T {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        (*self).write_query(writer, context, out);
    }
    fn is_ordered(&self) -> bool {
        (*self).is_ordered()
    }
    fn is_true(&self) -> bool {
        (*self).is_true()
    }
}

impl Expression for &dyn Expression {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        (*self).write_query(writer, context, out);
    }
    fn is_ordered(&self) -> bool {
        (*self).is_ordered()
    }
    fn is_true(&self) -> bool {
        (*self).is_true()
    }
}

impl Expression for () {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut String) {}
}

impl Expression for bool {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        writer.write_value_bool(context, out, *self);
    }
    fn is_true(&self) -> bool {
        *self
    }
}

impl Expression for &'static str {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        writer.write_value_string(context, out, self);
    }
}

impl<'a, T: Expression> From<&'a T> for &'a dyn Expression {
    fn from(value: &'a T) -> Self {
        value as &'a dyn Expression
    }
}

impl Expression for Value {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        writer.write_value(context, out, self);
    }
}
