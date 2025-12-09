use crate::{
    Action, BinaryOp, BinaryOpType, ColumnDef, ColumnRef, DataSet, EitherIterator, Entity,
    Expression, Fragment, Interval, Join, JoinType, Operand, Order, Ordered, PrimaryKeyType,
    TableRef, UnaryOp, UnaryOpType, Value, possibly_parenthesized, print_timer, separated_by,
    writer::Context,
};
use core::f64;
use futures::future::Either;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

macro_rules! write_integer {
    ($out:ident, $value:expr, $delim:expr) => {{
        $out.push_str($delim);
        let mut buffer = itoa::Buffer::new();
        $out.push_str(buffer.format($value));
        $out.push_str($delim);
    }};
}
macro_rules! write_float {
    ($this:ident, $context:ident,$out:ident, $value:expr, $delim:expr) => {{
        if $value.is_infinite() {
            $this.write_value_infinity($context, $out, $value.is_sign_negative());
        } else if $value.is_nan() {
            $this.write_value_nan($context, $out);
        } else {
            $out.push_str($delim);
            let mut buffer = ryu::Buffer::new();
            $out.push_str(buffer.format($value));
            $out.push_str($delim);
        }
    }};
}

/// Dialect printer converting semantic constructs into concrete SQL strings.
pub trait SqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter;

    /// Whether the current fragment context allows alias declaration.
    fn alias_declaration(&self, context: &mut Context) -> bool {
        match context.fragment {
            Fragment::SqlSelectFrom | Fragment::SqlJoin => true,
            _ => false,
        }
    }

    /// Escape occurrences of `search` char with `replace` while copying into buffer.
    fn write_escaped(
        &self,
        _context: &mut Context,
        out: &mut String,
        value: &str,
        search: char,
        replace: &str,
    ) {
        let mut position = 0;
        for (i, c) in value.char_indices() {
            if c == search {
                out.push_str(&value[position..i]);
                out.push_str(replace);
                position = i + 1;
            }
        }
        out.push_str(&value[position..]);
    }

    /// Quote identifiers ("name") doubling inner quotes.
    fn write_identifier_quoted(&self, context: &mut Context, out: &mut String, value: &str) {
        out.push('"');
        self.write_escaped(context, out, value, '"', "\"\"");
        out.push('"');
    }

    /// Render a table reference with optional alias.
    fn write_table_ref(&self, context: &mut Context, out: &mut String, value: &TableRef) {
        if self.alias_declaration(context) || value.alias.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier_quoted(context, out, &value.schema);
                out.push('.');
            }
            self.write_identifier_quoted(context, out, &value.name);
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }

    /// Render a column reference optionally qualifying with schema/table.
    fn write_column_ref(&self, context: &mut Context, out: &mut String, value: &ColumnRef) {
        if context.qualify_columns && !value.table.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier_quoted(context, out, &value.schema);
                out.push('.');
            }
            self.write_identifier_quoted(context, out, &value.table);
            out.push('.');
        }
        self.write_identifier_quoted(context, out, &value.name);
    }

    /// Render the SQL overridden type.
    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut String,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    /// Render the SQL type for a `Value` prototype.
    fn write_column_type(&self, context: &mut Context, out: &mut String, value: &Value) {
        match value {
            Value::Boolean(..) => out.push_str("BOOLEAN"),
            Value::Int8(..) => out.push_str("TINYINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INTEGER"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("HUGEINT"),
            Value::UInt8(..) => out.push_str("UTINYINT"),
            Value::UInt16(..) => out.push_str("USMALLINT"),
            Value::UInt32(..) => out.push_str("UINTEGER"),
            Value::UInt64(..) => out.push_str("UBIGINT"),
            Value::UInt128(..) => out.push_str("UHUGEINT"),
            Value::Float32(..) => out.push_str("FLOAT"),
            Value::Float64(..) => out.push_str("DOUBLE"),
            Value::Decimal(.., precision, scale) => {
                out.push_str("DECIMAL");
                if (precision, scale) != (&0, &0) {
                    let _ = write!(out, "({},{})", precision, scale);
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("VARCHAR"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMPTZ"),
            Value::Interval(..) => out.push_str("INTERVAL"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(.., inner, size) => {
                self.write_column_type(context, out, inner);
                let _ = write!(out, "[{}]", size);
            }
            Value::List(.., inner) => {
                self.write_column_type(context, out, inner);
                out.push_str("[]");
            }
            Value::Map(.., key, value) => {
                out.push_str("MAP(");
                self.write_column_type(context, out, key);
                out.push(',');
                self.write_column_type(context, out, value);
                out.push(')');
            }
            Value::Json(..) => out.push_str("JSON"),
            _ => log::error!(
                "Unexpected tank::Value, variant {:?} is not supported",
                value
            ),
        };
    }

    /// Render a concrete value (including proper quoting / escaping).
    fn write_value(&self, context: &mut Context, out: &mut String, value: &Value) {
        let delimiter = if context.fragment == Fragment::JsonKey {
            "\""
        } else {
            ""
        };
        match value {
            v if v.is_null() => self.write_value_none(context, out),
            Value::Boolean(Some(v), ..) => self.write_value_bool(context, out, *v),
            Value::Int8(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::Int16(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::Int32(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::Int64(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::Int128(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::UInt8(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::UInt16(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::UInt32(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::UInt64(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::UInt128(Some(v), ..) => write_integer!(out, *v, delimiter),
            Value::Float32(Some(v), ..) => write_float!(self, context, out, *v, delimiter),
            Value::Float64(Some(v), ..) => write_float!(self, context, out, *v, delimiter),
            Value::Decimal(Some(v), ..) => drop(write!(out, "{delimiter}{v}{delimiter}")),
            Value::Char(Some(v), ..) => {
                let mut buf = [0u8; 4];
                self.write_value_string(context, out, v.encode_utf8(&mut buf));
            }
            Value::Varchar(Some(v), ..) => self.write_value_string(context, out, v),
            Value::Blob(Some(v), ..) => self.write_value_blob(context, out, v.as_ref()),
            Value::Date(Some(v), ..) => self.write_value_date(context, out, v, false),
            Value::Time(Some(v), ..) => self.write_value_time(context, out, v, false),
            Value::Timestamp(Some(v), ..) => self.write_value_timestamp(context, out, v),
            Value::TimestampWithTimezone(Some(v), ..) => {
                self.write_value_timestamptz(context, out, v)
            }
            Value::Interval(Some(v), ..) => self.write_value_interval(context, out, v),
            Value::Uuid(Some(v), ..) => {
                let b = if context.is_inside_json() { '"' } else { '\'' };
                let _ = write!(out, "{b}{v}{b}");
            }
            Value::Array(Some(..), ..) | Value::List(Some(..), ..) => match value {
                Value::Array(Some(v), ..) => {
                    self.write_value_list(context, out, Either::Left(v), value)
                }
                Value::List(Some(v), ..) => {
                    self.write_value_list(context, out, Either::Right(v), value)
                }
                _ => unreachable!(),
            },
            Value::Map(Some(v), ..) => self.write_value_map(context, out, v),
            Value::Struct(Some(v), ..) => self.write_value_struct(context, out, v),
            _ => {
                log::error!("Cannot write {:?}", value);
            }
        };
    }

    /// Render NULL literal.
    fn write_value_none(&self, context: &mut Context, out: &mut String) {
        out.push_str(if context.fragment == Fragment::Json {
            "null"
        } else {
            "NULL"
        });
    }

    /// Render boolean literal.
    fn write_value_bool(&self, context: &mut Context, out: &mut String, value: bool) {
        if context.fragment == Fragment::JsonKey {
            out.push('"');
        }
        out.push_str(["false", "true"][value as usize]);
        if context.fragment == Fragment::JsonKey {
            out.push('"');
        }
    }

    /// Render +/- INF via CAST for dialect portability.
    fn write_value_infinity(&self, context: &mut Context, out: &mut String, negative: bool) {
        let mut buffer = ryu::Buffer::new();
        self.write_expression_binary_op(
            context,
            out,
            &BinaryOp {
                op: BinaryOpType::Cast,
                lhs: &Operand::LitStr(buffer.format(if negative {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                })),
                rhs: &Operand::Type(Value::Float64(None)),
            },
        );
    }

    /// Render NaN via CAST for dialect portability.
    fn write_value_nan(&self, context: &mut Context, out: &mut String) {
        let mut buffer = ryu::Buffer::new();
        self.write_expression_binary_op(
            context,
            out,
            &BinaryOp {
                op: BinaryOpType::Cast,
                lhs: &Operand::LitStr(buffer.format(f64::NAN)),
                rhs: &Operand::Type(Value::Float64(None)),
            },
        );
    }

    /// Render and escape a string literal using single quotes.
    fn write_value_string(&self, context: &mut Context, out: &mut String, value: &str) {
        let (delimiter, escaped) =
            if context.fragment == Fragment::Json || context.fragment == Fragment::JsonKey {
                ('"', r#"\""#)
            } else {
                ('\'', "''")
            };
        out.push(delimiter);
        let mut pos = 0;
        for (i, c) in value.char_indices() {
            if c == delimiter {
                out.push_str(&value[pos..i]);
                out.push_str(escaped);
                pos = i + 1;
            } else if c == '\n' {
                out.push_str(&value[pos..i]);
                out.push_str("\\n");
                pos = i + 1;
            }
        }
        out.push_str(&value[pos..]);
        out.push(delimiter);
    }

    /// Render a blob literal using hex escapes.
    fn write_value_blob(&self, context: &mut Context, out: &mut String, value: &[u8]) {
        let delimiter = if context.fragment == Fragment::Json {
            '"'
        } else {
            '\''
        };
        out.push(delimiter);
        for v in value {
            let _ = write!(out, "\\x{:X}", v);
        }
        out.push(delimiter);
    }

    /// Render a DATE literal (optionally as part of TIMESTAMP composition).
    fn write_value_date(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &Date,
        timestamp: bool,
    ) {
        let b = match context.fragment {
            Fragment::Json if !timestamp => "\"",
            _ if !timestamp => "'",
            _ => "",
        };
        let _ = write!(
            out,
            "{b}{:04}-{:02}-{:02}{b}",
            value.year(),
            value.month() as u8,
            value.day()
        );
    }

    /// Render a TIME literal (optionally as part of TIMESTAMP composition).
    fn write_value_time(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &Time,
        timestamp: bool,
    ) {
        print_timer(
            out,
            match context.fragment {
                Fragment::Json if !timestamp => "\"",
                _ if !timestamp => "'",
                _ => "",
            },
            value.hour() as _,
            value.minute(),
            value.second(),
            value.nanosecond(),
        );
    }

    /// Render a TIMESTAMP literal.
    fn write_value_timestamp(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &PrimitiveDateTime,
    ) {
        let delimiter = match context.fragment {
            Fragment::ParameterBinding => "",
            Fragment::Json => "\"",
            _ => "'",
        };
        out.push_str(delimiter);
        self.write_value_date(context, out, &value.date(), true);
        out.push('T');
        self.write_value_time(context, out, &value.time(), true);
        out.push_str(delimiter);
    }

    /// Render a TIMESTAMPTZ literal.
    fn write_value_timestamptz(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &OffsetDateTime,
    ) {
        let date_time = value.to_utc();
        self.write_value_timestamp(
            context,
            out,
            &PrimitiveDateTime::new(date_time.date(), date_time.time()),
        );
    }

    /// Ordered units used to decompose intervals.
    fn value_interval_units(&self) -> &[(&str, i128)] {
        static UNITS: &[(&str, i128)] = &[
            ("DAY", Interval::NANOS_IN_DAY),
            ("HOUR", Interval::NANOS_IN_SEC * 3600),
            ("MINUTE", Interval::NANOS_IN_SEC * 60),
            ("SECOND", Interval::NANOS_IN_SEC),
            ("MICROSECOND", 1_000),
            ("NANOSECOND", 1),
        ];
        UNITS
    }

    /// Render INTERVAL literal using largest representative units.
    fn write_value_interval(&self, context: &mut Context, out: &mut String, value: &Interval) {
        out.push_str("INTERVAL ");
        let delimiter = if context.fragment == Fragment::Json {
            '"'
        } else {
            '\''
        };
        out.push(delimiter);
        if value.is_zero() {
            out.push_str("0 SECONDS");
        }
        macro_rules! write_unit {
            ($out:ident, $len:ident, $val:expr, $unit:expr) => {
                if $out.len() > $len {
                    $out.push(' ');
                    $len = $out.len();
                }
                let _ = write!(
                    $out,
                    "{} {}{}",
                    $val,
                    $unit,
                    if $val != 1 { "S" } else { "" }
                );
            };
        }
        let mut months = value.months;
        let mut nanos = value.nanos + value.days as i128 * Interval::NANOS_IN_DAY;
        let mut len = out.len();
        if months != 0 {
            if months > 48 || months % 12 == 0 {
                write_unit!(out, len, months / 12, "YEAR");
                months = months % 12;
            }
            if months != 0 {
                write_unit!(out, len, months, "MONTH");
            }
        }
        for &(name, factor) in self.value_interval_units() {
            let rem = nanos % factor;
            if rem == 0 || factor / rem > 1_000_000 {
                let value = nanos / factor;
                if value != 0 {
                    write_unit!(out, len, value, name);
                    nanos = rem;
                    if nanos == 0 {
                        break;
                    }
                }
            }
        }
        out.push(delimiter);
    }

    /// Render list/array literal.
    fn write_value_list(
        &self,
        context: &mut Context,
        out: &mut String,
        value: Either<&Box<[Value]>, &Vec<Value>>,
        _ty: &Value,
    ) {
        out.push('[');
        separated_by(
            out,
            match value {
                Either::Left(v) => v.iter(),
                Either::Right(v) => v.iter(),
            },
            |out, v| {
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push(']');
    }

    /// Render map literal.
    fn write_value_map(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &HashMap<Value, Value>,
    ) {
        out.push('{');
        separated_by(
            out,
            value,
            |out, (k, v)| {
                self.write_value(context, out, k);
                out.push(':');
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push('}');
    }

    /// Render struct literal.
    fn write_value_struct(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &Vec<(String, Value)>,
    ) {
        out.push('{');
        separated_by(
            out,
            value,
            |out, (k, v)| {
                self.write_value_string(context, out, k);
                out.push(':');
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push('}');
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

    /// Render an operand (literal / variable / nested expression).
    fn write_expression_operand(&self, context: &mut Context, out: &mut String, value: &Operand) {
        let delimiter = if context.fragment == Fragment::JsonKey {
            "\""
        } else {
            ""
        };
        match value {
            Operand::LitBool(v) => self.write_value_bool(context, out, *v),
            Operand::LitFloat(v) => write_float!(self, context, out, *v, delimiter),
            Operand::LitIdent(v) => drop(out.push_str(v)),
            Operand::LitField(v) => separated_by(out, *v, |out, v| out.push_str(v), "."),
            Operand::LitInt(v) => write_integer!(out, *v, delimiter),
            Operand::LitStr(v) => self.write_value_string(context, out, v),
            Operand::LitArray(v) => {
                out.push('[');
                separated_by(
                    out,
                    *v,
                    |out, v| {
                        v.write_query(self.as_dyn(), context, out);
                    },
                    ", ",
                );
                out.push(']');
            }
            Operand::LitTuple(v) => {
                out.push('(');
                separated_by(
                    out,
                    *v,
                    |out, v| {
                        v.write_query(self.as_dyn(), context, out);
                    },
                    ", ",
                );
                out.push(')');
            }
            Operand::Null => drop(out.push_str("NULL")),
            Operand::Type(v) => self.write_column_type(context, out, v),
            Operand::Variable(v) => self.write_value(context, out, v),
            Operand::Call(f, args) => {
                out.push_str(f);
                out.push('(');
                separated_by(
                    out,
                    *args,
                    |out, v| {
                        v.write_query(self.as_dyn(), context, out);
                    },
                    ",",
                );
                out.push(')');
            }
            Operand::Asterisk => drop(out.push('*')),
            Operand::QuestionMark => self.write_expression_operand_question_mark(context, out),
        };
    }

    /// Render parameter placeholder (dialect may override).
    fn write_expression_operand_question_mark(&self, _context: &mut Context, out: &mut String) {
        out.push('?');
    }

    /// Render unary operator expression.
    fn write_expression_unary_op(
        &self,
        context: &mut Context,
        out: &mut String,
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

    /// Render binary operator expression handling precedence / parenthesis.
    fn write_expression_binary_op(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        let (prefix, infix, suffix, lhs_parenthesized, rhs_parenthesized) = match value.op {
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
            BinaryOpType::Alias => {
                if context.fragment == Fragment::SqlSelectOrderBy {
                    return value.lhs.write_query(self.as_dyn(), context, out);
                } else {
                    ("", " AS ", "", false, false)
                }
            }
        };
        let mut context = context.switch_fragment(if value.op == BinaryOpType::Cast {
            Fragment::Casting
        } else {
            context.fragment
        });
        let precedence = self.expression_binary_op_precedence(&value.op);
        out.push_str(prefix);
        possibly_parenthesized!(
            out,
            !lhs_parenthesized && value.lhs.precedence(self.as_dyn()) < precedence,
            value
                .lhs
                .write_query(self.as_dyn(), &mut context.current, out)
        );
        out.push_str(infix);
        possibly_parenthesized!(
            out,
            !rhs_parenthesized && value.rhs.precedence(self.as_dyn()) <= precedence,
            value
                .rhs
                .write_query(self.as_dyn(), &mut context.current, out)
        );
        out.push_str(suffix);
    }

    /// Render ordered expression inside ORDER BY.
    fn write_expression_ordered(
        &self,
        context: &mut Context,
        out: &mut String,
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

    /// Render join keyword(s) for the given join type.
    fn write_join_type(&self, _context: &mut Context, out: &mut String, join_type: &JoinType) {
        out.push_str(match &join_type {
            JoinType::Default => "JOIN",
            JoinType::Inner => "INNER JOIN",
            JoinType::Outer => "OUTER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Cross => "CROSS",
            JoinType::Natural => "NATURAL JOIN",
        });
    }

    /// Render a JOIN clause.
    fn write_join(
        &self,
        context: &mut Context,
        out: &mut String,
        join: &Join<&dyn DataSet, &dyn DataSet, &dyn Expression>,
    ) {
        let mut context = context.switch_fragment(Fragment::SqlJoin);
        context.current.qualify_columns = true;
        join.lhs
            .write_query(self.as_dyn(), &mut context.current, out);
        out.push(' ');
        self.write_join_type(&mut context.current, out, &join.join);
        out.push(' ');
        join.rhs
            .write_query(self.as_dyn(), &mut context.current, out);
        if let Some(on) = &join.on {
            out.push_str(" ON ");
            on.write_query(self.as_dyn(), &mut context.current, out);
        }
    }

    /// Emit BEGIN statement.
    fn write_transaction_begin(&self, out: &mut String) {
        out.push_str("BEGIN;");
    }

    /// Emit COMMIT statement.
    fn write_transaction_commit(&self, out: &mut String) {
        out.push_str("COMMIT;");
    }

    /// Emit ROLLBACK statement.
    fn write_transaction_rollback(&self, out: &mut String) {
        out.push_str("ROLLBACK;");
    }

    /// Emit CREATE SCHEMA.
    fn write_create_schema<E>(&self, out: &mut String, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(32 + E::table().schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE SCHEMA ");
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier_quoted(&mut context, out, E::table().schema);
        out.push(';');
    }

    /// Emit DROP SCHEMA.
    fn write_drop_schema<E>(&self, out: &mut String, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(24 + E::table().schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP SCHEMA ");
        let mut context = Context::new(Fragment::SqlDropSchema, E::qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier_quoted(&mut context, out, E::table().schema);
        out.push(';');
    }

    /// Emit CREATE TABLE with columns, constraints & comments.
    fn write_create_table<E>(&self, out: &mut String, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, E::qualified_columns());
        let estimated = 128 + E::columns().len() * 64 + E::primary_key_def().len() * 24;
        out.reserve(estimated);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE TABLE ");
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_table_ref(&mut context, out, E::table());
        out.push_str(" (\n");
        separated_by(
            out,
            E::columns(),
            |out, v| {
                self.write_create_table_column_fragment(&mut context, out, v);
            },
            ",\n",
        );
        let primary_key = E::primary_key_def();
        if primary_key.len() > 1 {
            out.push_str(",\nPRIMARY KEY (");
            separated_by(
                out,
                primary_key,
                |out, v| {
                    self.write_identifier_quoted(
                        &mut context
                            .switch_fragment(Fragment::SqlCreateTablePrimaryKey)
                            .current,
                        out,
                        v.name(),
                    );
                },
                ", ",
            );
            out.push(')');
        }
        for unique in E::unique_defs() {
            if unique.len() > 1 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique,
                    |out, v| {
                        self.write_identifier_quoted(
                            &mut context
                                .switch_fragment(Fragment::SqlCreateTableUnique)
                                .current,
                            out,
                            v.name(),
                        );
                    },
                    ", ",
                );
                out.push(')');
            }
        }
        let foreign_keys = E::columns().iter().filter(|c| c.references.is_some());
        separated_by(
            out,
            foreign_keys,
            |out, column| {
                let references = column.references.unwrap();
                out.push_str(",\nFOREIGN KEY (");
                self.write_identifier_quoted(&mut context, out, &column.name());
                out.push_str(") REFERENCES ");
                self.write_table_ref(&mut context, out, &references.table());
                out.push('(');
                self.write_column_ref(&mut context, out, &references);
                out.push(')');
                if let Some(on_delete) = &column.on_delete {
                    out.push_str(" ON DELETE ");
                    self.write_create_table_references_action(&mut context, out, on_delete);
                }
                if let Some(on_update) = &column.on_update {
                    out.push_str(" ON UPDATE ");
                    self.write_create_table_references_action(&mut context, out, on_update);
                }
            },
            "",
        );
        out.push_str(");");
        self.write_column_comments_statements::<E>(&mut context, out);
    }

    /// Emit single column definition fragment.
    fn write_create_table_column_fragment(
        &self,
        context: &mut Context,
        out: &mut String,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        self.write_identifier_quoted(context, out, &column.name());
        out.push(' ');
        let len = out.len();
        self.write_column_overridden_type(context, out, column, &column.column_type);
        let didnt_write_type = out.len() == len;
        if didnt_write_type {
            SqlWriter::write_column_type(self, context, out, &column.value);
        }
        if !column.nullable && column.primary_key == PrimaryKeyType::None {
            out.push_str(" NOT NULL");
        }
        if let Some(default) = &column.default {
            out.push_str(" DEFAULT ");
            default.write_query(self.as_dyn(), context, out);
        }
        if column.primary_key == PrimaryKeyType::PrimaryKey {
            // Composite primary key will be printed elsewhere
            out.push_str(" PRIMARY KEY");
        }
        if column.unique && column.primary_key != PrimaryKeyType::PrimaryKey {
            out.push_str(" UNIQUE");
        }
        if !column.comment.is_empty() {
            self.write_column_comment_inline(context, out, column);
        }
    }

    /// Emit referential action keyword.
    fn write_create_table_references_action(
        &self,
        _context: &mut Context,
        out: &mut String,
        action: &Action,
    ) {
        out.push_str(match action {
            Action::NoAction => "NO ACTION",
            Action::Restrict => "RESTRICT",
            Action::Cascade => "CASCADE",
            Action::SetNull => "SET NULL",
            Action::SetDefault => "SET DEFAULT",
        });
    }

    fn write_column_comment_inline(
        &self,
        _context: &mut Context,
        _out: &mut String,
        _column: &ColumnDef,
    ) where
        Self: Sized,
    {
    }

    /// Emit COMMENT ON COLUMN statements for columns carrying comments.
    fn write_column_comments_statements<E>(&self, context: &mut Context, out: &mut String)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = context.switch_fragment(Fragment::SqlCommentOnColumn);
        context.current.qualify_columns = true;
        for c in E::columns().iter().filter(|c| !c.comment.is_empty()) {
            out.push_str("\nCOMMENT ON COLUMN ");
            self.write_column_ref(&mut context.current, out, c.into());
            out.push_str(" IS ");
            self.write_value_string(&mut context.current, out, c.comment);
            out.push(';');
        }
    }

    /// Emit DROP TABLE statement.
    fn write_drop_table<E>(&self, out: &mut String, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(24 + E::table().schema.len() + E::table().name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP TABLE ");
        let mut context = Context::new(Fragment::SqlDropTable, E::qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_table_ref(&mut context, out, E::table());
        out.push(';');
    }

    /// Emit SELECT statement (projection, FROM, WHERE, ORDER, LIMIT).
    fn write_select<Item, Cols, Data, Cond>(
        &self,
        out: &mut String,
        columns: Cols,
        from: &Data,
        condition: &Cond,
        limit: Option<u32>,
    ) where
        Self: Sized,
        Item: Expression,
        Cols: IntoIterator<Item = Item> + Clone,
        Data: DataSet,
        Cond: Expression,
    {
        let cols = columns.clone().into_iter().count();
        out.reserve(128 + cols * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        let mut has_order_by = false;
        let mut context = Context::new(Fragment::SqlSelect, Data::qualified_columns());
        separated_by(
            out,
            columns.clone(),
            |out, col| {
                col.write_query(self, &mut context, out);
                has_order_by = has_order_by || col.is_ordered();
            },
            ", ",
        );
        out.push_str("\nFROM ");
        from.write_query(
            self,
            &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
            out,
        );
        out.push_str("\nWHERE ");
        condition.write_query(
            self,
            &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
            out,
        );
        if has_order_by {
            out.push_str("\nORDER BY ");
            let mut order_context = context.switch_fragment(Fragment::SqlSelectOrderBy);
            separated_by(
                out,
                columns.into_iter().filter(Expression::is_ordered),
                |out, col| {
                    col.write_query(self, &mut order_context.current, out);
                },
                ", ",
            );
        }
        if let Some(limit) = limit {
            let _ = write!(out, "\nLIMIT {}", limit);
        }
        out.push(';');
    }

    /// Emit INSERT (single/multi-row) optionally with ON CONFLICT DO UPDATE.
    fn write_insert<'b, E>(
        &self,
        out: &mut String,
        entities: impl IntoIterator<Item = &'b E>,
        update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let mut rows = entities.into_iter().map(Entity::row_filtered).peekable();
        let Some(mut row) = rows.next() else {
            return;
        };
        let cols = E::columns().len();
        out.reserve(128 + cols * 48);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("INSERT INTO ");
        let mut context = Context::new(Fragment::SqlInsertInto, E::qualified_columns());
        self.write_table_ref(&mut context, out, E::table());
        out.push_str(" (");
        let columns = E::columns().iter();
        let single = rows.peek().is_none();
        if single {
            // Inserting a single row uses row_labeled to filter out Passive::NotSet columns
            separated_by(
                out,
                row.iter(),
                |out, v| {
                    self.write_identifier_quoted(&mut context, out, v.0);
                },
                ", ",
            );
        } else {
            separated_by(
                out,
                columns.clone(),
                |out, v| {
                    self.write_identifier_quoted(&mut context, out, v.name());
                },
                ", ",
            );
        };
        out.push_str(") VALUES\n");
        let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
        let mut first_row = None;
        let mut separate = false;
        loop {
            if separate {
                out.push_str(",\n");
            }
            out.push('(');
            let mut fields = row.iter();
            let mut field = fields.next();
            separated_by(
                out,
                E::columns(),
                |out, col| {
                    if Some(col.name()) == field.map(|v| v.0) {
                        self.write_value(
                            &mut context.current,
                            out,
                            field
                                .map(|v| &v.1)
                                .expect(&format!("Column {} does not have a value", col.name())),
                        );
                        field = fields.next();
                    } else if !single {
                        out.push_str("DEFAULT");
                    }
                },
                ", ",
            );
            out.push(')');
            separate = true;
            if first_row.is_none() {
                first_row = row.into();
            }
            if let Some(next) = rows.next() {
                row = next;
            } else {
                break;
            };
        }
        let first_row = first_row
            .expect("Should have at least one row")
            .into_iter()
            .map(|(v, _)| v);
        if update {
            self.write_insert_update_fragment::<E>(
                &mut context.current,
                out,
                if single {
                    EitherIterator::Left(
                        // If there is only one row to insert then list only the columns that appear
                        columns.filter(|c| first_row.clone().find(|n| *n == c.name()).is_some()),
                    )
                } else {
                    EitherIterator::Right(columns)
                },
            );
        }
        out.push(';');
    }

    /// Emit ON CONFLICT DO UPDATE fragment for upsert.
    fn write_insert_update_fragment<'a, E>(
        &self,
        context: &mut Context,
        out: &mut String,
        columns: impl Iterator<Item = &'a ColumnDef>,
    ) where
        Self: Sized,
        E: Entity,
    {
        let pk = E::primary_key_def();
        if pk.len() == 0 {
            return;
        }
        out.push_str("\nON CONFLICT");
        context.fragment = Fragment::SqlInsertIntoOnConflict;
        if pk.len() > 0 {
            out.push_str(" (");
            separated_by(
                out,
                pk,
                |out, v| {
                    self.write_identifier_quoted(context, out, v.name());
                },
                ", ",
            );
            out.push(')');
        }
        out.push_str(" DO UPDATE SET\n");
        separated_by(
            out,
            columns.filter(|c| c.primary_key == PrimaryKeyType::None),
            |out, v| {
                self.write_identifier_quoted(context, out, v.name());
                out.push_str(" = EXCLUDED.");
                self.write_identifier_quoted(context, out, v.name());
            },
            ",\n",
        );
    }

    /// Emit DELETE statement with WHERE clause.
    fn write_delete<E>(&self, out: &mut String, condition: &impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(128 + E::table().schema.len() + E::table().name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, E::qualified_columns());
        self.write_table_ref(&mut context, out, E::table());
        out.push_str("\nWHERE ");
        condition.write_query(
            self,
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            out,
        );
        out.push(';');
    }
}

/// Fallback generic SQL writer (closest to PostgreSQL / DuckDB conventions).
pub struct GenericSqlWriter;
impl GenericSqlWriter {
    /// Construct a new generic writer.
    pub fn new() -> Self {
        Self {}
    }
}
impl SqlWriter for GenericSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
}
