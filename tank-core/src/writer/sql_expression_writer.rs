use crate::*;
use std::fmt::Write;

/// Expression rendering: operands, operators, precedence and functions.
pub trait SqlExpressionWriter: SqlValueWriter {
    fn write_function(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        function: &str,
        args: &[&dyn Expression],
    ) {
        out.push_str(function);
        out.push('(');
        separated_by(
            out,
            args,
            |out, expr| {
                expr.write_query(self.as_dyn(), context, out);
            },
            ",",
        );
        out.push(')');
    }

    /// Precedence table for unary operators.
    fn expression_unary_op_precedence(&self, value: &UnaryOpType) -> i32 {
        match value {
            UnaryOpType::Negative => 1250,
            UnaryOpType::Not => 250,
        }
    }

    /// Precedence table for binary operators.
    fn expression_binary_op_precedence(&self, value: &BinaryOpType) -> i32 {
        match value {
            BinaryOpType::Or => 100,
            BinaryOpType::And => 200,
            BinaryOpType::Equal => 300,
            BinaryOpType::NotEqual => 300,
            BinaryOpType::Less => 300,
            BinaryOpType::Greater => 300,
            BinaryOpType::LessEqual => 300,
            BinaryOpType::GreaterEqual => 300,
            BinaryOpType::In => 400,
            BinaryOpType::NotIn => 400,
            BinaryOpType::Is => 400,
            BinaryOpType::IsNot => 400,
            BinaryOpType::Like => 400,
            BinaryOpType::NotLike => 400,
            BinaryOpType::Regexp => 400,
            BinaryOpType::NotRegexp => 400,
            BinaryOpType::Glob => 400,
            BinaryOpType::NotGlob => 400,
            BinaryOpType::BitwiseOr => 500,
            BinaryOpType::BitwiseAnd => 600,
            BinaryOpType::ShiftLeft => 700,
            BinaryOpType::ShiftRight => 700,
            BinaryOpType::Subtraction => 800,
            BinaryOpType::Addition => 800,
            BinaryOpType::Multiplication => 900,
            BinaryOpType::Division => 900,
            BinaryOpType::Remainder => 900,
            BinaryOpType::Indexing => 1000,
            BinaryOpType::Cast => 1100,
            BinaryOpType::Alias => 1200,
        }
    }

    fn expression_binary_op_fragments(
        &self,
        _context: &mut Context,
        op_type: BinaryOpType,
    ) -> (&str, &str, &str, bool, bool) {
        match op_type {
            BinaryOpType::Indexing => ("", "[", "]", false, true),
            BinaryOpType::Cast => ("CAST(", " AS ", ")", true, true),
            BinaryOpType::Multiplication => ("", " * ", "", false, false),
            BinaryOpType::Division => ("", " / ", "", false, false),
            BinaryOpType::Remainder => ("", " % ", "", false, false),
            BinaryOpType::Addition => ("", " + ", "", false, false),
            BinaryOpType::Subtraction => ("", " - ", "", false, false),
            BinaryOpType::ShiftLeft => ("", " << ", "", false, false),
            BinaryOpType::ShiftRight => ("", " >> ", "", false, false),
            BinaryOpType::BitwiseAnd => ("", " & ", "", false, false),
            BinaryOpType::BitwiseOr => ("", " | ", "", false, false),
            BinaryOpType::In => ("", " IN ", "", false, false),
            BinaryOpType::NotIn => ("", " NOT IN ", "", false, false),
            BinaryOpType::Is => ("", " IS ", "", false, false),
            BinaryOpType::IsNot => ("", " IS NOT ", "", false, false),
            BinaryOpType::Like => ("", " LIKE ", "", false, false),
            BinaryOpType::NotLike => ("", " NOT LIKE ", "", false, false),
            BinaryOpType::Regexp => ("", " REGEXP ", "", false, false),
            BinaryOpType::NotRegexp => ("", " NOT REGEXP ", "", false, false),
            BinaryOpType::Glob => ("", " GLOB ", "", false, false),
            BinaryOpType::NotGlob => ("", " NOT GLOB ", "", false, false),
            BinaryOpType::Equal => ("", " = ", "", false, false),
            BinaryOpType::NotEqual => ("", " != ", "", false, false),
            BinaryOpType::Less => ("", " < ", "", false, false),
            BinaryOpType::LessEqual => ("", " <= ", "", false, false),
            BinaryOpType::Greater => ("", " > ", "", false, false),
            BinaryOpType::GreaterEqual => ("", " >= ", "", false, false),
            BinaryOpType::And => ("", " AND ", "", false, false),
            BinaryOpType::Or => ("", " OR ", "", false, false),
            BinaryOpType::Alias => ("", " AS ", "", false, false),
        }
    }

    fn write_operand(&self, context: &mut Context, out: &mut DynQuery, value: &Operand) {
        match value {
            Operand::Null => self.write_null(context, out),
            Operand::LitBool(v) => self.write_bool(context, out, *v),
            Operand::LitInt(v) => self.write_value_i128(context, out, *v),
            Operand::LitFloat(v) => self.write_value_f64(context, out, *v),
            Operand::LitStr(v) => self.write_string(context, out, v),
            Operand::LitIdent(v) => {
                self.write_identifier(context, out, v, context.fragment == Fragment::Aliasing)
            }
            Operand::LitField(v) => {
                let separator = self.separator();
                for (i, part) in v.iter().enumerate() {
                    if i > 0 {
                        out.push_str(separator);
                    }
                    out.push_str(part);
                }
            }
            Operand::LitList(v) => self.write_list(
                context,
                out,
                &mut v.iter().map(|v| v as &dyn Expression),
                None,
                false,
            ),
            Operand::LitTuple(v) => {
                self.write_tuple(context, out, &mut v.iter().map(|v| v as &dyn Expression))
            }
            Operand::Type(v) => self.write_column_type(context, out, v),
            Operand::Variable(v) => self.write_value(context, out, v),
            Operand::Value(v) => self.write_value(context, out, v),
            Operand::Call(f, args) => self.write_function(context, out, f, args),
            Operand::Asterisk => drop(out.push('*')),
            Operand::QuestionMark => self.write_question_mark(context, out),
            Operand::CurrentTimestampMs => self.write_current_timestamp_ms(context, out),
        };
    }

    fn write_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        context.counter += 1;
        out.push('?');
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        out.push_str("NOW()");
    }

    fn write_unary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) {
        match value.op {
            UnaryOpType::Negative => out.push('-'),
            UnaryOpType::Not => out.push_str("NOT "),
        };
        possibly_parenthesized!(
            out,
            value.arg.precedence(self.as_dyn()) <= self.expression_unary_op_precedence(&value.op),
            value.arg.write_query(self.as_dyn(), context, out)
        );
    }

    /// Render binary operator expression handling precedence and parenthesis.
    fn write_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        if value.op == BinaryOpType::Alias
            && matches!(
                context.fragment,
                Fragment::SqlSelectOrderBy | Fragment::SqlSelectGroupBy
            )
        {
            return value.lhs.write_query(self.as_dyn(), context, out);
        }
        if matches!(value.op, BinaryOpType::In | BinaryOpType::NotIn) {
            // An empty collection cannot be rendered as an (invalid) `IN ()`, so
            // emit a constant condition rather than letting the list collapse.
            struct IsEmptyCollection;
            impl ExpressionVisitor for IsEmptyCollection {
                fn visit_operand(
                    &mut self,
                    _writer: &dyn SqlWriter,
                    _context: &mut Context,
                    _out: &mut DynQuery,
                    value: &Operand,
                ) -> bool {
                    match value {
                        Operand::LitList(values) | Operand::LitTuple(values) => values.is_empty(),
                        Operand::Variable(Value::Array(Some(values), ..))
                        | Operand::Value(Value::Array(Some(values), ..)) => values.is_empty(),
                        Operand::Variable(Value::List(Some(values), ..))
                        | Operand::Value(Value::List(Some(values), ..)) => values.is_empty(),
                        _ => false,
                    }
                }
            }
            if value.rhs.accept_visitor(
                &mut IsEmptyCollection,
                self.as_dyn(),
                context,
                &mut Default::default(),
            ) {
                out.push_str(if value.op == BinaryOpType::In {
                    "FALSE"
                } else {
                    "TRUE"
                });
                return;
            }
        }
        let (prefix, infix, suffix, lhs_parenthesized, rhs_parenthesized) =
            self.expression_binary_op_fragments(context, value.op);
        let precedence = self.expression_binary_op_precedence(&value.op);
        out.push_str(prefix);
        possibly_parenthesized!(
            out,
            !lhs_parenthesized && value.lhs.precedence(self.as_dyn()) < precedence,
            value.lhs.write_query(self.as_dyn(), context, out)
        );
        out.push_str(infix);
        let mut context = context.switch_fragment(match value.op {
            BinaryOpType::Cast => Fragment::Casting,
            BinaryOpType::Alias => Fragment::Aliasing,
            _ => context.fragment,
        });
        if matches!(value.op, BinaryOpType::In | BinaryOpType::NotIn) {
            // Expands a Rust collection (`#collection as IN`), carried as
            // [`Value::Array`]/[`Value::List`], into a parenthesized list instead
            // of the SQL array literal it would otherwise render as.
            struct WriteInList;
            impl ExpressionVisitor for WriteInList {
                fn visit_operand(
                    &mut self,
                    writer: &dyn SqlWriter,
                    context: &mut Context,
                    out: &mut DynQuery,
                    value: &Operand,
                ) -> bool {
                    fn write(
                        writer: &dyn SqlWriter,
                        context: &mut Context,
                        out: &mut DynQuery,
                        v: &[Value],
                    ) {
                        writer.write_tuple(
                            context,
                            out,
                            &mut v.iter().map(|v| v as &dyn Expression),
                        );
                    }
                    match value {
                        Operand::LitList(values) | Operand::LitTuple(values) => writer.write_tuple(
                            context,
                            out,
                            &mut values.iter().map(|v| v as &dyn Expression),
                        ),
                        Operand::Variable(Value::Array(Some(values), ..))
                        | Operand::Value(Value::Array(Some(values), ..)) => {
                            write(writer, context, out, values)
                        }
                        Operand::Variable(Value::List(Some(values), ..))
                        | Operand::Value(Value::List(Some(values), ..)) => {
                            write(writer, context, out, values)
                        }
                        _ => return false,
                    }
                    true
                }
            }
            if value
                .rhs
                .accept_visitor(&mut WriteInList, self.as_dyn(), &mut context.current, out)
            {
                out.push_str(suffix);
                return;
            }
        }
        possibly_parenthesized!(
            out,
            !rhs_parenthesized && value.rhs.precedence(self.as_dyn()) <= precedence,
            value
                .rhs
                .write_query(self.as_dyn(), &mut context.current, out)
        );
        out.push_str(suffix);
    }

    fn write_cast(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        expr: &dyn Expression,
        ty: &dyn Expression,
    ) {
        let mut context = context.switch_fragment(Fragment::Casting);
        out.push_str("CAST(");
        expr.write_query(self.as_dyn(), &mut context.current, out);
        out.push_str(" AS ");
        ty.write_query(self.as_dyn(), &mut context.current, out);
        out.push(')');
    }

    /// Render ordered expression inside ORDER BY.
    fn write_ordered(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Ordered<&dyn Expression>,
    ) {
        value.expression.write_query(self.as_dyn(), context, out);
        if context.fragment == Fragment::SqlSelectOrderBy {
            let _ = write!(
                out,
                " {}",
                match value.order {
                    Order::ASC => "ASC",
                    Order::DESC => "DESC",
                }
            );
        }
    }
}
