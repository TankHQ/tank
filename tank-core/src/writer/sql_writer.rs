use crate::{
    Action, BinaryOp, BinaryOpType, ColumnDef, ColumnRef, Dataset, DynQuery, Entity, Expression,
    Fragment, Interval, IsTrue, Join, JoinType, Operand, Order, Ordered, PrimaryKeyType,
    SelectQuery, TableRef, UnaryOp, UnaryOpType, Value, possibly_parenthesized, separated_by,
    write_escaped, writer::Context,
};
use core::f64;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
    mem,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

macro_rules! write_integer_fn {
    ($fn_name:ident, $ty:ty) => {
        fn $fn_name(&self, context: &mut Context, out: &mut DynQuery, value: $ty) {
            if context.fragment == Fragment::JsonKey {
                out.push('"');
            }
            let mut buffer = itoa::Buffer::new();
            out.push_str(buffer.format(value));
            if context.fragment == Fragment::JsonKey {
                out.push('"');
            }
        }
    };
}

macro_rules! write_float_fn {
    ($fn_name:ident, $ty:ty) => {
        fn $fn_name(&self, context: &mut Context, out: &mut DynQuery, value: $ty) {
            let mut buffer = ryu::Buffer::new();
            if value.is_infinite() {
                self.write_binary_op(
                    context,
                    out,
                    &BinaryOp {
                        op: BinaryOpType::Cast,
                        lhs: &Operand::LitStr(buffer.format(if value.is_sign_negative() {
                            f64::NEG_INFINITY
                        } else {
                            f64::INFINITY
                        })),
                        rhs: &Operand::Type(Value::Float64(None)),
                    },
                );
            } else if value.is_nan() {
                self.write_binary_op(
                    context,
                    out,
                    &BinaryOp {
                        op: BinaryOpType::Cast,
                        lhs: &Operand::LitStr(buffer.format(f64::NAN)),
                        rhs: &Operand::Type(Value::Float64(None)),
                    },
                );
            } else {
                if context.fragment == Fragment::JsonKey {
                    out.push('"');
                }
                out.push_str(buffer.format(value));
                if context.fragment == Fragment::JsonKey {
                    out.push('"');
                }
            }
        }
    };
}

/// SQL dialect printer.
pub trait SqlWriter: Send {
    /// Upcasts self to a distinct dynamic object.
    fn as_dyn(&self) -> &dyn SqlWriter;

    /// Separator used for qualified names (e.g., schema.table.column)
    fn separator(&self) -> &str {
        "."
    }

    /// Determines if the current SQL context supports alias declarations.
    fn is_alias_declaration(&self, context: &mut Context) -> bool {
        match context.fragment {
            Fragment::SqlSelectFrom | Fragment::SqlJoin => true,
            _ => false,
        }
    }

    /// Writes an identifier (like a table or column name) to the query builder, optionally quoting it.
    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &str,
        quoted: bool,
    ) {
        if quoted {
            out.push('"');
            write_escaped(out, value, '"', "\"\"");
            out.push('"');
        } else {
            out.push_str(value);
        }
    }

    /// Write table reference.
    fn write_table_ref(&self, context: &mut Context, out: &mut DynQuery, value: &TableRef) {
        if self.is_alias_declaration(context) || value.alias.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier(context, out, &value.schema, context.quote_identifiers);
                out.push_str(self.separator());
            }
            self.write_identifier(context, out, &value.name, context.quote_identifiers);
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }

    /// Write column reference.
    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        if context.qualify_columns {
            let table_ref = mem::take(&mut context.table_ref);
            let mut schema = &table_ref.schema;
            if schema.is_empty() {
                schema = &value.schema;
            }
            let mut table = &table_ref.alias;
            if table.is_empty() {
                table = &table_ref.name;
            }
            if table.is_empty() {
                table = &value.table;
            }
            if !table.is_empty() {
                if !schema.is_empty() {
                    self.write_identifier(context, out, schema, context.quote_identifiers);
                    out.push_str(self.separator());
                }
                self.write_identifier(context, out, table, context.quote_identifiers);
                out.push_str(self.separator());
            }
            context.table_ref = table_ref
        }
        self.write_identifier(context, out, &value.name, context.quote_identifiers);
    }

    /// Write overridden type.
    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
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

    /// Write SQL type name.
    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
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
                    let _ = write!(out, "({precision},{scale})");
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
                let _ = write!(out, "[{size}]");
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
            _ => log::error!("Unexpected tank::Value, variant {value:?} is not supported"),
        };
    }

    /// Write value.
    fn write_value(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
        let delimiter = if context.fragment == Fragment::JsonKey {
            "\""
        } else {
            ""
        };
        match value {
            v if v.is_null() => self.write_null(context, out),
            Value::Boolean(Some(v), ..) => self.write_bool(context, out, *v),
            Value::Int8(Some(v), ..) => self.write_value_i8(context, out, *v),
            Value::Int16(Some(v), ..) => self.write_value_i16(context, out, *v),
            Value::Int32(Some(v), ..) => self.write_value_i32(context, out, *v),
            Value::Int64(Some(v), ..) => self.write_value_i64(context, out, *v),
            Value::Int128(Some(v), ..) => self.write_value_i128(context, out, *v),
            Value::UInt8(Some(v), ..) => self.write_value_u8(context, out, *v),
            Value::UInt16(Some(v), ..) => self.write_value_u16(context, out, *v),
            Value::UInt32(Some(v), ..) => self.write_value_u32(context, out, *v),
            Value::UInt64(Some(v), ..) => self.write_value_u64(context, out, *v),
            Value::UInt128(Some(v), ..) => self.write_value_u128(context, out, *v),
            Value::Float32(Some(v), ..) => self.write_value_f32(context, out, *v),
            Value::Float64(Some(v), ..) => self.write_value_f64(context, out, *v),
            Value::Decimal(Some(v), ..) => drop(write!(out, "{delimiter}{v}{delimiter}")),
            Value::Char(Some(v), ..) => {
                let mut buf = [0u8; 4];
                self.write_string(context, out, v.encode_utf8(&mut buf));
            }
            Value::Varchar(Some(v), ..) => self.write_string(context, out, v),
            Value::Blob(Some(v), ..) => self.write_blob(context, out, v.as_ref()),
            Value::Date(Some(v), ..) => self.write_date(context, out, v),
            Value::Time(Some(v), ..) => self.write_time(context, out, v),
            Value::Timestamp(Some(v), ..) => self.write_timestamp(context, out, v),
            Value::TimestampWithTimezone(Some(v), ..) => self.write_timestamptz(context, out, v),
            Value::Interval(Some(v), ..) => self.write_interval(context, out, v),
            Value::Uuid(Some(v), ..) => self.write_uuid(context, out, v),
            Value::Array(Some(..), elem_ty, ..) | Value::List(Some(..), elem_ty, ..) => match value
            {
                Value::Array(Some(v), ..) => self.write_list(
                    context,
                    out,
                    &mut v.iter().map(|v| v as &dyn Expression),
                    Some(&*elem_ty),
                    true,
                ),
                Value::List(Some(v), ..) => self.write_list(
                    context,
                    out,
                    &mut v.iter().map(|v| v as &dyn Expression),
                    Some(&*elem_ty),
                    false,
                ),
                _ => unreachable!(),
            },
            Value::Map(Some(v), ..) => self.write_map(context, out, v),
            Value::Json(Some(v), ..) => self.write_json(context, out, v),
            Value::Struct(Some(v), ..) => self.write_struct(context, out, v),
            _ => {
                log::error!("Cannot write {value:?}");
            }
        };
    }

    fn write_null(&self, context: &mut Context, out: &mut DynQuery) {
        out.push_str(if context.fragment == Fragment::Json {
            "null"
        } else {
            "NULL"
        });
    }

    fn write_bool(&self, context: &mut Context, out: &mut DynQuery, value: bool) {
        if context.fragment == Fragment::JsonKey {
            out.push('"');
        }
        out.push_str(["false", "true"][value as usize]);
        if context.fragment == Fragment::JsonKey {
            out.push('"');
        }
    }

    write_integer_fn!(write_value_i8, i8);
    write_integer_fn!(write_value_i16, i16);
    write_integer_fn!(write_value_i32, i32);
    write_integer_fn!(write_value_i64, i64);
    write_integer_fn!(write_value_i128, i128);
    write_integer_fn!(write_value_u8, u8);
    write_integer_fn!(write_value_u16, u16);
    write_integer_fn!(write_value_u32, u32);
    write_integer_fn!(write_value_u64, u64);
    write_integer_fn!(write_value_u128, u128);

    write_float_fn!(write_value_f32, f32);
    write_float_fn!(write_value_f64, f64);

    fn write_string(&self, context: &mut Context, out: &mut DynQuery, value: &str) {
        let (delimiter, escaped) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => (None, ""),
            Fragment::Json | Fragment::JsonKey => (Some('"'), r#"\""#),
            _ => (Some('\''), "''"),
        };
        if let Some(delimiter) = delimiter {
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
        } else {
            out.push_str(value);
        }
    }

    fn write_blob(&self, context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        let delimiter = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        out.push_str(delimiter);
        for v in value {
            let _ = write!(out, "\\x{:02X}", v);
        }
        out.push_str(delimiter);
    }

    fn write_date(&self, context: &mut Context, out: &mut DynQuery, value: &Date) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let year = value.year();
        let month = value.month() as u8;
        let day = value.day();
        let _ = write!(out, "{d}{year:04}-{month:02}-{day:02}{d}");
    }

    fn write_time(&self, context: &mut Context, out: &mut DynQuery, value: &Time) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let (h, m, s, ns) = value.as_hms_nano();
        let mut subsecond = ns;
        let mut width = 9;
        while width > 1 && subsecond % 10 == 0 {
            subsecond /= 10;
            width -= 1;
        }
        let _ = write!(out, "{d}{h:02}:{m:02}:{s:02}.{subsecond:0width$}{d}");
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(d);
        self.write_date(&mut context.current, out, &value.date());
        out.push(' ');
        self.write_time(&mut context.current, out, &value.time());
        out.push_str(d);
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut DynQuery, value: &OffsetDateTime) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(d);
        self.write_timestamp(
            &mut context.current,
            out,
            &PrimitiveDateTime::new(value.date(), value.time()),
        );
        let (h, m, s) = value.offset().as_hms();
        if h != 0 || m != 0 || s != 0 {
            out.push(if h >= 0 { '+' } else { '-' });
            let _ = write!(out, "{h:02}");
            if m != 0 || s != 0 {
                let _ = write!(out, ":{m:02}");
                if s != 0 {
                    let _ = write!(out, ":{s:02}");
                }
            }
        }
        out.push_str(d);
    }

    /// Units used to decompose intervals (notice the decreasing order).
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

    fn write_interval(&self, context: &mut Context, out: &mut DynQuery, value: &Interval) {
        out.push_str("INTERVAL ");
        let d = match context.fragment {
            Fragment::None => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        out.push_str(d);
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
        out.push_str(d);
    }

    fn write_uuid(&self, context: &mut Context, out: &mut DynQuery, value: &Uuid) {
        let d = match context.fragment {
            Fragment::None => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let _ = write!(out, "{d}{value}{d}");
    }

    fn write_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
        _ty: Option<&Value>,
        _is_array: bool,
    ) {
        out.push('[');
        separated_by(
            out,
            value,
            |out, v| {
                v.write_query(self.as_dyn(), context, out);
            },
            ",",
        );
        out.push(']');
    }

    fn write_tuple(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
    ) {
        out.push('(');
        separated_by(
            out,
            value,
            |out, v| {
                v.write_query(self.as_dyn(), context, out);
            },
            ",",
        );
        out.push(')');
    }

    fn write_map(&self, context: &mut Context, out: &mut DynQuery, value: &HashMap<Value, Value>) {
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

    fn write_json(&self, context: &mut Context, out: &mut DynQuery, value: &serde_json::Value) {
        self.write_string(context, out, &value.to_string());
    }

    fn write_struct(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Vec<(String, Value)>,
    ) {
        out.push('{');
        separated_by(
            out,
            value,
            |out, (k, v)| {
                self.write_string(context, out, k);
                out.push(':');
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push('}');
    }

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
                self.write_identifier(context, out, &v.join(self.separator()), false)
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
        let (prefix, infix, suffix, lhs_parenthesized, rhs_parenthesized) = match value.op {
            BinaryOpType::Indexing => ("", "[", "]", false, true),
            BinaryOpType::Cast => {
                return self.write_cast(context, out, value.lhs, value.rhs);
            }
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
        let precedence = self.expression_binary_op_precedence(&value.op);
        out.push_str(prefix);
        possibly_parenthesized!(
            out,
            !lhs_parenthesized && value.lhs.precedence(self.as_dyn()) < precedence,
            value.lhs.write_query(self.as_dyn(), context, out)
        );
        out.push_str(infix);
        let mut context = context.switch_fragment(if value.op == BinaryOpType::Alias {
            Fragment::Aliasing
        } else {
            context.fragment
        });
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

    /// Render join keyword(s) for the given join type.
    fn write_join_type(&self, _context: &mut Context, out: &mut DynQuery, join_type: &JoinType) {
        out.push_str(match &join_type {
            JoinType::Default => "JOIN",
            JoinType::Inner => "INNER JOIN",
            JoinType::Outer => "OUTER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Cross => "CROSS JOIN",
            JoinType::Natural => "NATURAL JOIN",
        });
    }

    /// Render a JOIN clause.
    fn write_join(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        join: &Join<&dyn Dataset, &dyn Dataset, &dyn Expression>,
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
    fn write_transaction_begin(&self, out: &mut DynQuery) {
        out.push_str("BEGIN;");
    }

    /// Emit COMMIT statement.
    fn write_transaction_commit(&self, out: &mut DynQuery) {
        out.push_str("COMMIT;");
    }

    /// Emit ROLLBACK statement.
    fn write_transaction_rollback(&self, out: &mut DynQuery) {
        out.push_str("ROLLBACK;");
    }

    /// Emit CREATE SCHEMA.
    fn write_create_schema<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE SCHEMA ");
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit DROP SCHEMA.
    fn write_drop_schema<E>(&self, out: &mut DynQuery, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlDropSchema, E::qualified_columns());
        let table = E::table();
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP SCHEMA ");
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit CREATE TABLE with columns, constraints & comments.
    fn write_create_table<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, E::qualified_columns());
        let table = E::table();
        let estimated = 128 + E::columns().len() * 64 + E::primary_key_def().len() * 24;
        out.buffer().reserve(estimated);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE TABLE ");
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push_str(" (\n");
        separated_by(
            out,
            E::columns(),
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        let pk = E::primary_key_def();
        if pk.len() > 1 {
            self.write_create_table_primary_key_fragment(&mut context, out, pk.iter().map(|v| *v));
        }
        for unique in E::unique_defs() {
            if unique.len() > 1 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique,
                    |out, col| {
                        self.write_identifier(
                            &mut context
                                .switch_fragment(Fragment::SqlCreateTableUnique)
                                .current,
                            out,
                            col.name(),
                            true,
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
                let references = column.references.as_ref().unwrap();
                out.push_str(",\nFOREIGN KEY (");
                self.write_identifier(&mut context, out, &column.name(), true);
                out.push_str(") REFERENCES ");
                self.write_table_ref(&mut context, out, &references.table());
                out.push('(');
                self.write_column_ref(&mut context, out, references);
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
        out: &mut DynQuery,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        self.write_identifier(context, out, &column.name(), true);
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
        if column.default.is_set() {
            out.push_str(" DEFAULT ");
            column.default.write_query(self.as_dyn(), context, out);
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

    /// Write PRIMARY KEY constraint.
    fn write_create_table_primary_key_fragment<'a, It>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        primary_key: It,
    ) where
        Self: Sized,
        It: IntoIterator<Item = &'a ColumnDef>,
        It::IntoIter: Clone,
    {
        out.push_str(",\nPRIMARY KEY (");
        separated_by(
            out,
            primary_key,
            |out, col| {
                self.write_identifier(
                    &mut context
                        .switch_fragment(Fragment::SqlCreateTablePrimaryKey)
                        .current,
                    out,
                    col.name(),
                    true,
                );
            },
            ", ",
        );
        out.push(')');
    }

    /// Write referential action.
    fn write_create_table_references_action(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
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
        _out: &mut DynQuery,
        _column: &ColumnDef,
    ) where
        Self: Sized,
    {
    }

    /// Write column comments.
    fn write_column_comments_statements<E>(&self, context: &mut Context, out: &mut DynQuery)
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
            self.write_string(&mut context.current, out, c.comment);
            out.push(';');
        }
    }

    /// Write DROP TABLE statement.
    fn write_drop_table<E>(&self, out: &mut DynQuery, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer()
            .reserve(24 + table.schema.len() + table.name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP TABLE ");
        let mut context = Context::new(Fragment::SqlDropTable, E::qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push(';');
    }

    /// Write SELECT statement.
    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let Some(from) = query.get_from() else {
            log::error!("The query does not have the FROM clause");
            return;
        };
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        out.buffer().reserve(128 + columns_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        let mut context = Context::new(Fragment::SqlSelect, Data::qualified_columns());
        if columns_count != 0 {
            separated_by(
                out,
                columns.clone(),
                |out, col| {
                    col.write_query(self, &mut context, out);
                },
                ", ",
            );
        } else {
            out.push('*');
        }
        out.push_str("\nFROM ");
        from.write_query(
            self,
            &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
            out,
        );
        if let Some(condition) = query.get_where()
            && !condition.accept_visitor(&mut IsTrue, self, &mut context, out)
        {
            out.push_str("\nWHERE ");
            condition.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
                out,
            );
        }
        let mut group_by = query.get_group_by().peekable();
        if group_by.peek().is_some() {
            out.push_str("\nGROUP BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectGroupBy);
            separated_by(
                out,
                group_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        if let Some(having) = query.get_having() {
            out.push_str("\nHAVING ");
            having.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
                out,
            );
        }
        let mut order_by = query.get_order_by().peekable();
        if order_by.peek().is_some() {
            out.push_str("\nORDER BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectOrderBy);
            separated_by(
                out,
                order_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        if let Some(limit) = query.get_limit() {
            let _ = write!(out, "\nLIMIT {limit}");
        }
        out.push(';');
    }

    /// Write INSERT statement.
    fn write_insert<'b, E>(
        &self,
        out: &mut DynQuery,
        entities: impl IntoIterator<Item = &'b E>,
        update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let table = E::table();
        let mut entities = entities.into_iter().peekable();
        if entities.peek().is_none() {
            return;
        };
        let cols = E::columns().len();
        out.buffer().reserve(128 + cols * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("INSERT INTO ");
        let mut context = Context::new(Fragment::SqlInsertInto, E::qualified_columns());
        self.write_table_ref(&mut context, out, table);
        out.push_str(" (");
        separated_by(
            out,
            E::columns().iter(),
            |out, col| {
                self.write_identifier(&mut context, out, col.name(), true);
            },
            ", ",
        );
        out.push_str(") VALUES");
        let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
        separated_by(
            out,
            entities,
            |out, entity| {
                out.push_str("\n(");
                separated_by(
                    out,
                    entity.row_full(),
                    |out, value| self.write_value(&mut context.current, out, &value),
                    ", ",
                );
                out.push(')');
            },
            ",",
        );
        if update {
            self.write_insert_update_fragment::<E>(&mut context.current, out, E::columns().iter());
        }
        out.push(';');
    }

    /// Write ON CONFLICT DO UPDATE fragment for upsert.
    fn write_insert_update_fragment<'a, E>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        columns: impl Iterator<Item = &'a ColumnDef> + Clone,
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
                |out, col| {
                    self.write_identifier(context, out, col.name(), true);
                },
                ", ",
            );
            out.push(')');
        }
        let mut update_cols = columns
            .filter(|c| c.primary_key == PrimaryKeyType::None)
            .peekable();
        if update_cols.peek().is_some() {
            out.push_str(" DO UPDATE SET\n");
            separated_by(
                out,
                update_cols,
                |out, col| {
                    self.write_identifier(context, out, col.name(), true);
                    out.push_str(" = EXCLUDED.");
                    self.write_identifier(context, out, col.name(), true);
                },
                ",\n",
            );
        } else {
            out.push_str(" DO NOTHING");
        }
    }

    /// Write DELETE statement.
    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer().reserve(128);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, E::qualified_columns());
        self.write_table_ref(&mut context, out, table);
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

/// Generic SQL writer.
pub struct GenericSqlWriter;
impl GenericSqlWriter {
    /// New generic writer.
    pub fn new() -> Self {
        Self {}
    }
}
impl SqlWriter for GenericSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
}
