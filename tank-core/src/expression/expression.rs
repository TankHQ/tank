use crate::{
    writer::{Context, SqlWriter},
    AsValue, DynQuery, ExpressionVisitor, FixedDecimal, GenericSqlWriter, Interval, OpPrecedence,
    Operand, Value,
};
use rust_decimal::Decimal;
use std::{borrow::Cow, mem, sync::Arc};
use time::UtcDateTime;
use uuid::Uuid;

/// Renderable SQL expression.
pub trait Expression: OpPrecedence {
    /// Generates the SQL representation of this expression.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery);
    /// Traverses the expression with a visitor.
    ///
    /// Returns `true` if the visitor matched this node.
    fn accept_visitor(
        &self,
        _matcher: &mut dyn ExpressionVisitor,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
    ) -> bool {
        false
    }
    /// Renders the expression as a string suitable for use as an identifier.
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
}

impl Expression for bool {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_bool(context, out, *self);
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
        writer.write_string(context, out, self);
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

macro_rules! impl_expression {
    ($($T:ty),* $(,)?) => {$(
        impl OpPrecedence for $T {
            fn precedence(&self, _: &dyn SqlWriter) -> i32 { 0 }
        }
        impl Expression for $T {
            fn write_query(
                &self,
                writer: &dyn SqlWriter,
                context: &mut Context,
                out: &mut DynQuery,
            ) {
                writer.write_value(context, out, &(*self).as_value());
            }
        }
    )*};
}

impl_expression!(
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    f32,
    f64,
    char,
    Uuid,
    Decimal,
    Interval,
    std::time::Duration,
    time::Duration,
    time::Date,
    time::Time,
    time::PrimitiveDateTime,
    time::OffsetDateTime,
);

#[cfg(feature = "chrono")]
impl_expression!(
    chrono::NaiveDate,
    chrono::NaiveTime,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::FixedOffset>,
);

impl OpPrecedence for UtcDateTime {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl Expression for UtcDateTime {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(
            context,
            out,
            &Value::Timestamp(Some(time::PrimitiveDateTime::new(self.date(), self.time()))),
        );
    }
}

impl OpPrecedence for String {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl Expression for String {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_string(context, out, self);
    }
}

impl OpPrecedence for Cow<'static, str> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl Expression for Cow<'static, str> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_string(context, out, self.as_ref());
    }
}

impl OpPrecedence for Box<[u8]> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl Expression for Box<[u8]> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, &Value::Blob(Some(self.clone())));
    }
}

impl OpPrecedence for serde_json::Value {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl Expression for serde_json::Value {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, &Value::Json(Some(self.clone())));
    }
}

impl<const W: u8, const S: u8> OpPrecedence for FixedDecimal<W, S> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl<const W: u8, const S: u8> Expression for FixedDecimal<W, S> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_value(context, out, &(*self).as_value());
    }
}

impl<T: Expression> OpPrecedence for Arc<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl<T: Expression> Expression for Arc<T> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (**self).write_query(writer, context, out);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        (**self).accept_visitor(matcher, writer, context, out)
    }
}

impl<T: Expression> OpPrecedence for Box<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl<T: Expression> Expression for Box<T> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (**self).write_query(writer, context, out);
    }
    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        (**self).accept_visitor(matcher, writer, context, out)
    }
}

impl<T: AsValue + Expression> OpPrecedence for Option<T> {
    fn precedence(&self, _: &dyn SqlWriter) -> i32 {
        0
    }
}
impl<T: AsValue + Expression> Expression for Option<T> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        match self {
            Some(v) => v.write_query(writer, context, out),
            None => writer.write_value(context, out, &T::as_empty_value()),
        }
    }
}
