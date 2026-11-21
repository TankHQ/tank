use crate::{AsValue, Expression, FixedDecimal, Value, writer::SqlWriter};
use std::{borrow::Cow, sync::Arc};
use time::UtcDateTime;

/// Provides numeric precedence for expressions allowing sql writers to insert parentheses.
pub trait OpPrecedence {
    /// Writers parenthesize when child precedence <= operator precedence.
    fn precedence(&self, writer: &dyn SqlWriter) -> i32;
}

impl OpPrecedence for () {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        1_000_000_000
    }
}

impl<T: OpPrecedence> OpPrecedence for &T {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        (*self).precedence(writer)
    }
}

impl OpPrecedence for &dyn Expression {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        (*self).precedence(writer)
    }
}

impl OpPrecedence for bool {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        0
    }
}

impl<const W: u8, const S: u8> OpPrecedence for FixedDecimal<W, S> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for &'_ str {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for String {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for Cow<'_, str> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for Box<[u8]> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for Value<'_> {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for Cow<'_, Value<'_>> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for serde_json::Value {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl OpPrecedence for UtcDateTime {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl<T: AsValue + Expression> OpPrecedence for Option<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl<T: Expression> OpPrecedence for Box<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}

impl<T: Expression> OpPrecedence for Arc<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
