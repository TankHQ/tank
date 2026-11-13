use crate::{
    BinaryOp, BinaryOpType, DynQuery, Error, Expression, Fragment, Interval, Operand, Value,
    separated_by, write_escaped, write_zero_padded,
    writer::{Context, SqlCoreWriter},
};
use rust_decimal::Decimal;
use std::{collections::HashMap, fmt::Write};
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
                self.as_dyn().write_binary_op(
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
                self.as_dyn().write_binary_op(
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

/// Literal value rendering for the SQL dialect.
pub trait SqlValueWriter: SqlCoreWriter {
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
}
