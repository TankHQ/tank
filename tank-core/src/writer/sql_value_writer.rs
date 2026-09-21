use crate::*;
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

/// Value (literal) rendering for the SQL dialect.
pub trait SqlValueWriter: SqlCoreWriter {
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

    write_float_fn!(write_value_f32, f32, Value::Float32(None));
    write_float_fn!(write_value_f64, f64, Value::Float64(None));

    fn write_string(&self, context: &mut Context, out: &mut DynQuery, value: &str) {
        if matches!(context.fragment, Fragment::Json | Fragment::JsonKey) {
            match serde_json::to_string(value) {
                Ok(s) => out.push_str(&s),
                Err(e) => {
                    let error = Error::new(e).context("Failed to serialize string as JSON");
                    log::error!("{error:#}");
                }
            }
            return;
        }
        let (delimiter, escaped) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => (None, ""),
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
        let _ = write!(
            out,
            "{d}{}{:04}-{month:02}-{day:02}{d}",
            if year < 0 { "-" } else { "" },
            year.unsigned_abs()
        );
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
            let is_negative = h < 0 || (h == 0 && (m < 0 || (m == 0 && s < 0)));
            out.push(if is_negative { '-' } else { '+' });
            let _ = write!(out, "{:02}", h.unsigned_abs());
            if m != 0 || s != 0 {
                let _ = write!(out, ":{:02}", m.unsigned_abs());
                if s != 0 {
                    let _ = write!(out, ":{:02}", s.unsigned_abs());
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
                    if $val != 1 && $val != -1 { "S" } else { "" }
                );
            };
        }
        let mut months = value.months;
        let mut nanos = value.nanos + value.days as i128 * Interval::NANOS_IN_DAY;
        let mut len = out.len();
        if months != 0 {
            if months.abs() > 48 || months % 12 == 0 {
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

    fn write_struct(&self, context: &mut Context, out: &mut DynQuery, value: &[(String, Value)]) {
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
