use crate::{
    DynQuery, Expression, ExpressionVisitor, OpPrecedence,
    writer::{Context, SqlWriter},
};

/// Expression that produces `CASE` statements, usually written with `if` or `match` inside a `expr!(...)`.
///
/// ```ignore
/// expr!(if Order::amount > 100.0 { "large" } else { "small" })
/// // CASE WHEN "amount" > 100.0 THEN 'large' ELSE 'small' END
///
/// expr!(match Order::status { 1 => "open", 2 | 3 => "closed", _ => "unknown" })
/// // CASE "status" WHEN 1 THEN 'open' WHEN 2 THEN 'closed' WHEN 3 THEN 'closed' ELSE 'unknown' END
/// ```
pub struct Case<'a> {
    pub value: Option<&'a dyn Expression>,
    pub when: &'a [(&'a dyn Expression, &'a dyn Expression)],
    pub otherwise: Option<&'a dyn Expression>,
}

impl OpPrecedence for Case<'_> {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        // Delimited by `CASE` .. `END`, never needs parentheses.
        1_000_000
    }
}

impl Expression for Case<'_> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_case(context, out, self);
    }

    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_case(writer, context, out, self)
    }
}
