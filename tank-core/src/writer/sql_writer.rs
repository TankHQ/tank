use crate::{
    Action, BinaryOp, BinaryOpType, Case, ColumnDef, ColumnRef, Dataset, DynQuery, Error,
    Expression, Fragment, InsertIntoQuery, Interval, IsTrue, Join, JoinType, Operand, Order,
    Ordered, PrimaryKeyType, SelectQuery, TableRef, UnaryOp, UnaryOpType, Value, WriteInList,
    future::Either, possibly_parenthesized, separated_by, write_escaped, write_zero_padded,
    writer::Context,
};
use rust_decimal::Decimal;
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
    ($fn_name:ident, $ty:ty, $value_ty:expr) => {
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
                        rhs: &Operand::Type($value_ty),
                    },
                );
            } else if value.is_nan() {
                self.write_binary_op(
                    context,
                    out,
                    &BinaryOp {
                        op: BinaryOpType::Cast,
                        lhs: &Operand::LitStr(buffer.format(f64::NAN)),
                        rhs: &Operand::Type($value_ty),
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
        let alias_declaration = self.is_alias_declaration(context);
        if alias_declaration || value.alias.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier(context, out, &value.schema, context.quote_identifiers);
                out.push_str(self.separator());
            }
            self.write_identifier(context, out, &value.name, context.quote_identifiers);
        }
        if !value.alias.is_empty() {
            if alias_declaration {
                out.push(' ');
            }
            out.push_str(&value.alias);
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
    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value<'_>) {
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
            Value::Decimal(_, precision, scale, ..) => {
                out.push_str("DECIMAL");
                if (*precision, *scale) != (0, 0) {
                    out.push('(');
                    out.push_str(itoa::Buffer::new().format(*precision));
                    out.push(',');
                    out.push_str(itoa::Buffer::new().format(*scale));
                    out.push(')');
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMPTZ"),
            Value::Interval(..) => out.push_str("INTERVAL"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(_, t, ..) => {
                self.write_column_type(context, out, &t.element);
                out.push('[');
                out.push_str(itoa::Buffer::new().format(t.len));
                out.push(']');
            }
            Value::List(_, t, ..) => {
                self.write_column_type(context, out, &t.element);
                out.push_str("[]");
            }
            Value::Map(_, t, ..) => {
                out.push_str("MAP(");
                self.write_column_type(context, out, &t.key);
                out.push(',');
                self.write_column_type(context, out, &t.value);
                out.push(')');
            }
            Value::Json(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, variant {value:?} is not supported"),
        };
    }

    /// Write value.
    fn write_value(&self, context: &mut Context, out: &mut DynQuery, value: &Value<'_>) {
        match value {
            Value::Boolean(Some(v), ..) => self.write_bool(context, out, *v),
            Value::Int8(Some(v), ..) => self.write_value_i8(context, out, *v),
            Value::Int16(Some(v), ..) => self.write_value_i16(context, out, *v),
            Value::Int32(Some(v), ..) => self.write_value_i32(context, out, *v),
            Value::Int64(Some(v), ..) => self.write_value_i64(context, out, *v),
            Value::Int128(Some(v), ..) => self.write_value_i128(context, out, **v),
            Value::UInt8(Some(v), ..) => self.write_value_u8(context, out, *v),
            Value::UInt16(Some(v), ..) => self.write_value_u16(context, out, *v),
            Value::UInt32(Some(v), ..) => self.write_value_u32(context, out, *v),
            Value::UInt64(Some(v), ..) => self.write_value_u64(context, out, *v),
            Value::UInt128(Some(v), ..) => self.write_value_u128(context, out, **v),
            Value::Float32(Some(v), ..) => self.write_value_f32(context, out, *v),
            Value::Float64(Some(v), ..) => self.write_value_f64(context, out, *v),
            Value::Decimal(Some(v), ..) => self.write_decimal(context, out, v),
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
            Value::Array(Some(v), t, ..) | Value::List(Some(v), t, ..) => self.write_list(
                context,
                out,
                &mut v.iter().map(|v| v as &dyn Expression),
                Some(&t.element),
                matches!(value, Value::Array(..)),
            ),
            Value::Map(Some(v), ..) => self.write_map(context, out, v),
            Value::Json(Some(v), ..) if !v.is_null() => self.write_json(context, out, v),
            Value::Struct(Some(v), ..) => self.write_struct(context, out, v),
            _ if value.is_null() => self.write_null(context, out),
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

    write_float_fn!(write_value_f32, f32, Value::Float32(None));
    write_float_fn!(write_value_f64, f64, Value::Float64(None));

    fn write_decimal(&self, context: &mut Context, out: &mut DynQuery, value: &Decimal) {
        let b = if context.fragment == Fragment::JsonKey {
            "\""
        } else {
            ""
        };
        out.push_str(b);
        let _ = write!(out, "{value}");
        out.push_str(b);
    }

    fn write_string(&self, context: &mut Context, out: &mut DynQuery, value: &str) {
        if matches!(context.fragment, Fragment::Json | Fragment::JsonKey) {
            let buffer = unsafe { out.buffer().as_mut_vec() };
            if let Err(e) = serde_json::to_writer(buffer, value) {
                let error = Error::new(e).context("Failed to serialize string as JSON");
                log::error!("{error:#}");
            }
            return;
        }
        let (b, escaped) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => (None, ""),
            _ => (Some('\''), "''"),
        };
        if let Some(b) = b {
            out.push(b);
            write_escaped(out, value, b, escaped);
            out.push(b);
        } else {
            out.push_str(value);
        }
    }

    fn write_blob(&self, context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        let b = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        out.buffer().reserve(4 * value.len() + 2 * b.len());
        out.push_str(b);
        for &v in value {
            out.push_str("\\x");
            out.push(HEX[(v >> 4) as usize] as char); // Upper 2 HEX
            out.push(HEX[(v & 0xF) as usize] as char); // Lower 2 HEX
        }
        out.push_str(b);
    }

    fn write_date(&self, context: &mut Context, out: &mut DynQuery, value: &Date) {
        let b = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let (year, month, day) = value.to_calendar_date();
        out.push_str(b);
        if year < 0 {
            out.push('-');
        }
        write_zero_padded(out, year.unsigned_abs(), 4);
        out.push('-');
        write_zero_padded(out, month as u32, 2);
        out.push('-');
        write_zero_padded(out, day as u32, 2);
        out.push_str(b);
    }

    fn write_time(&self, context: &mut Context, out: &mut DynQuery, value: &Time) {
        let b = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let (hour, min, sec, mut subsecond) = value.as_hms_nano();
        let mut width = 9;
        while width > 1 && subsecond % 10 == 0 {
            subsecond /= 10;
            width -= 1;
        }
        out.push_str(b);
        write_zero_padded(out, hour as u32, 2);
        out.push(':');
        write_zero_padded(out, min as u32, 2);
        out.push(':');
        write_zero_padded(out, sec as u32, 2);
        out.push('.');
        write_zero_padded(out, subsecond, width);
        out.push_str(b);
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let b = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(b);
        self.write_date(&mut context.current, out, &value.date());
        out.push(' ');
        self.write_time(&mut context.current, out, &value.time());
        out.push_str(b);
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut DynQuery, value: &OffsetDateTime) {
        let b = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(b);
        self.write_timestamp(
            &mut context.current,
            out,
            &PrimitiveDateTime::new(value.date(), value.time()),
        );
        let (h, m, s) = value.offset().as_hms();
        if h != 0 || m != 0 || s != 0 {
            let is_negative = h < 0 || (h == 0 && (m < 0 || (m == 0 && s < 0)));
            out.push(if is_negative { '-' } else { '+' });
            write_zero_padded(out, h.unsigned_abs() as u32, 2);
            if m != 0 || s != 0 {
                out.push(':');
                write_zero_padded(out, m.unsigned_abs() as u32, 2);
                if s != 0 {
                    out.push(':');
                    write_zero_padded(out, s.unsigned_abs() as u32, 2);
                }
            }
        }
        out.push_str(b);
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
        let b = match context.fragment {
            Fragment::None => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        out.push_str(b);
        if value.is_zero() {
            out.push_str("0 SECONDS");
        }
        macro_rules! write_unit {
            ($out:ident, $len:ident, $val:expr, $unit:expr) => {
                if $out.len() > $len {
                    $out.push(' ');
                    $len = $out.len();
                }
                $out.push_str(itoa::Buffer::new().format($val));
                $out.push(' ');
                $out.push_str($unit);
                if $val != 1 && $val != -1 {
                    $out.push('S');
                }
            };
        }
        let mut months = value.months;
        let mut nanos = value.nanos as i128 + value.days as i128 * Interval::NANOS_IN_DAY;
        let mut len = out.len();
        if months != 0 {
            if months.unsigned_abs() > 48 || months % 12 == 0 {
                write_unit!(out, len, months / 12, "YEAR");
                months = months % 12;
            }
            if months != 0 {
                write_unit!(out, len, months, "MONTH");
            }
        }
        for &(name, factor) in self.value_interval_units() {
            let rem = nanos % factor;
            if rem == 0 || (rem != 0 && factor / rem.abs() > 1_000_000) {
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
        out.push_str(b);
    }

    fn write_uuid(&self, context: &mut Context, out: &mut DynQuery, value: &Uuid) {
        let b = match context.fragment {
            Fragment::None => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        out.push_str(b);
        out.push_str(value.hyphenated().encode_lower(&mut Uuid::encode_buffer()));
        out.push_str(b);
    }

    fn write_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
        _t: Option<&Value<'_>>,
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

    fn write_map(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &HashMap<Value<'_>, Value<'_>>,
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

    fn write_json(&self, context: &mut Context, out: &mut DynQuery, value: &serde_json::Value) {
        self.write_string(context, out, &value.to_string());
    }

    fn write_struct(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &[(String, Value<'_>)],
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

    fn expression_binary_op_fragments(
        &self,
        _context: &Context,
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
        // Standard SQL does not allow select-list aliases in GROUP BY/ORDER BY.
        if value.op == BinaryOpType::Alias
            && matches!(
                context.fragment,
                Fragment::SqlSelectOrderBy | Fragment::SqlSelectGroupBy
            )
        {
            return value.lhs.write_query(self.as_dyn(), context, out);
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
        let mut switched;
        let context = match value.op {
            BinaryOpType::Cast | BinaryOpType::Alias => {
                switched = context.switch_fragment(if value.op == BinaryOpType::Cast {
                    Fragment::Casting
                } else {
                    Fragment::Aliasing
                });
                &mut switched.current
            }
            _ => context,
        };
        if matches!(value.op, BinaryOpType::In | BinaryOpType::NotIn)
            && value
                .rhs
                .accept_visitor(&mut WriteInList, self.as_dyn(), context, out)
        {
            out.push_str(suffix);
        } else {
            possibly_parenthesized!(
                out,
                !rhs_parenthesized && value.rhs.precedence(self.as_dyn()) <= precedence,
                value.rhs.write_query(self.as_dyn(), context, out)
            );
            out.push_str(suffix);
        }
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

    /// Render a `CASE` expression.
    fn write_case(&self, context: &mut Context, out: &mut DynQuery, value: &Case) {
        out.push_str("CASE");
        if let Some(v) = value.value {
            out.push(' ');
            v.write_query(self.as_dyn(), context, out);
        }
        for (when, then) in value.when {
            out.push_str(" WHEN ");
            when.write_query(self.as_dyn(), context, out);
            out.push_str(" THEN ");
            then.write_query(self.as_dyn(), context, out);
        }
        if let Some(otherwise) = value.otherwise {
            out.push_str(" ELSE ");
            otherwise.write_query(self.as_dyn(), context, out);
        }
        out.push_str(" END");
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
            out.push_str(match value.order {
                Order::ASC => " ASC",
                Order::DESC => " DESC",
            });
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
            .write_table_name(self.as_dyn(), &mut context.current, out);
        out.push(' ');
        self.write_join_type(&mut context.current, out, &join.join);
        out.push(' ');
        join.rhs
            .write_table_name(self.as_dyn(), &mut context.current, out);
        if let Some(on) = &join.on {
            out.push_str(" ON ");
            on.write_query(self.as_dyn(), &mut context.current, out);
        }
    }

    /// Emit BEGIN statement.
    fn write_transaction_begin(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("BEGIN;");
    }

    /// Emit COMMIT statement.
    fn write_transaction_commit(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("COMMIT;");
    }

    /// Emit ROLLBACK statement.
    fn write_transaction_rollback(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("ROLLBACK;");
    }

    /// Emit CREATE SCHEMA.
    fn write_create_schema(&self, out: &mut DynQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE SCHEMA ");
        let mut context = Context::new(Fragment::SqlCreateSchema, table.qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit DROP SCHEMA.
    fn write_drop_schema(&self, out: &mut DynQuery, table: &TableRef, if_exists: bool)
    where
        Self: Sized,
    {
        let mut context = Context::new(Fragment::SqlDropSchema, table.qualified_columns());
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

    /// Emit CREATE TABLE with columns and constraints.
    fn write_create_table(&self, out: &mut DynQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, table.qualified_columns());
        let estimated = 128 + table.columns.len() * 64 + table.primary_key.len() * 24;
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
            table.columns,
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        let pk = table.primary_key;
        if pk.len() > 1 {
            self.write_create_table_primary_key_fragment(&mut context, out, pk.iter().map(|v| *v));
        }
        for unique in table.unique {
            if unique.len() > 0 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique.iter(),
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
        let foreign_keys = table.columns.iter().filter(|c| c.references.is_some());
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

    /// Write the `COMMENT ON COLUMN` statement of a single column.
    fn write_column_comment(&self, context: &mut Context, out: &mut DynQuery, column: &ColumnDef) {
        let mut context = context.switch_fragment(Fragment::SqlCommentOnColumn);
        context.current.qualify_columns = true;
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("COMMENT ON COLUMN ");
        self.write_column_ref(&mut context.current, out, column.into());
        out.push_str(" IS ");
        self.write_string(&mut context.current, out, column.comment);
        out.push(';');
    }

    /// Write DROP TABLE statement.
    fn write_drop_table(&self, out: &mut DynQuery, table: &TableRef, if_exists: bool)
    where
        Self: Sized,
    {
        out.buffer()
            .reserve(32 + table.schema.len() + table.name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP TABLE ");
        let mut context = Context::new(Fragment::SqlDropTable, table.qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push(';');
    }

    /// Write SELECT statement.
    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        out.buffer().reserve(256 + columns_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        let qualified = query
            .get_from()
            .as_ref()
            .is_some_and(Dataset::qualified_columns);
        let mut context = Context::new(Fragment::SqlSelect, qualified);
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
        if let Some(from) = query.get_from() {
            out.push_str("\nFROM ");
            from.write_table_name(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
                out,
            );
        }
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
                &mut context.switch_fragment(Fragment::SqlSelectHaving).current,
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
            out.push_str("\nLIMIT ");
            out.push_str(itoa::Buffer::new().format(limit));
        }
        out.push(';');
    }

    /// Write INSERT statement printing rows of values or a SELECT.
    fn write_insert(&self, out: &mut DynQuery, query: impl InsertIntoQuery)
    where
        Self: Sized,
    {
        let table = query.get_table();
        let columns = query.get_columns();
        // A SELECT without named columns fills all the table columns, rows always name theirs.
        let write_head = |out: &mut DynQuery, context: &mut Context, all: bool| {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("INSERT INTO ");
            self.write_table_ref(context, out, table);
            if !all || !columns.is_empty() {
                out.push_str(" (");
                separated_by(
                    out,
                    columns,
                    |out, column| self.write_identifier(context, out, column.name(), true),
                    ", ",
                );
                out.push(')');
            }
        };
        let mut context = Context::new(Fragment::SqlInsertInto, table.qualified_columns());
        let rows = match query.get_values() {
            Either::Left(rows) => rows,
            Either::Right(select) => {
                write_head(out, &mut context, true);
                return self.write_select(out, select);
            }
        };
        let mut rows = rows.peekable();
        if rows.peek().is_none() {
            return;
        }
        out.buffer()
            .reserve(128 + columns.len() * (32 + rows.size_hint().0 * 16));
        write_head(out, &mut context, false);
        out.push_str(" VALUES");
        let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
        separated_by(
            out,
            rows,
            |out, row| {
                out.push_str("\n(");
                separated_by(
                    out,
                    row,
                    |out, value| self.write_value(&mut context.current, out, &value),
                    ", ",
                );
                out.push(')');
            },
            ",",
        );
        if query.get_update() {
            self.write_insert_update_fragment(
                &mut context.current,
                out,
                table.primary_key.iter().copied(),
                columns.iter(),
            );
        }
        out.push(';');
    }

    /// Write ON CONFLICT DO UPDATE fragment for upsert, updating the `columns` not in the `primary_key`.
    fn write_insert_update_fragment<'a>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        primary_key: impl Iterator<Item = &'a ColumnDef> + Clone,
        columns: impl Iterator<Item = &'a ColumnDef> + Clone,
    ) where
        Self: Sized,
    {
        let mut pk = primary_key.clone().peekable();
        if pk.peek().is_none() {
            return;
        }
        out.push_str("\nON CONFLICT");
        context.fragment = Fragment::SqlInsertIntoOnConflict;
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
        let mut update_cols = columns
            .filter(|c| !primary_key.clone().any(|pk| pk.name() == c.name()))
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
    fn write_delete(&self, out: &mut DynQuery, from: &dyn Dataset, condition: impl Expression)
    where
        Self: Sized,
    {
        out.buffer().reserve(128);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, from.qualified_columns());
        from.write_table_name(self, &mut context, out);
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
