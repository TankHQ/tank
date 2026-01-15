#![allow(unused_imports)]
use crate::{
    Error, FixedDecimal, Interval, Passive, Result, Value, consume_while, extract_number,
    month_to_number, number_to_month, print_date, print_timer, truncate_long,
};
use anyhow::Context;
#[cfg(feature = "chrono")]
use chrono::{Datelike, Timelike};
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};
use std::{
    any, array,
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap, LinkedList, VecDeque},
    hash::Hash,
    rc::Rc,
    sync::{Arc, RwLock},
};
use time::{Month, PrimitiveDateTime, Time, format_description::parse_borrowed};
use uuid::Uuid;

/// Convert both ways between Rust types and `Value` (plus simple parsing).
pub trait AsValue {
    /// Return a NULL equivalent variant for this type.
    fn as_empty_value() -> Value;
    /// Convert into owned `Value`.
    fn as_value(self) -> Value;
    /// Try to convert a dynamic `Value` into `Self`.
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized;
    /// Parse a full string into `Self`.
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        Err(Error::msg(format!(
            "Cannot parse '{}' as {} (the parse method is not implemented)",
            truncate_long!(input.as_ref()),
            any::type_name::<Self>()
        )))
    }
}

impl AsValue for Value {
    fn as_empty_value() -> Value {
        Value::Null
    }
    fn as_value(self) -> Value {
        self
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(value)
    }
}

impl From<&'static str> for Value {
    fn from(value: &'static str) -> Self {
        Value::Varchar(Some(value.into()))
    }
}

macro_rules! impl_as_value {
    ($source:ty, $destination:path $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        impl AsValue for $source {
            fn as_empty_value() -> Value {
                $destination(None)
            }
            fn as_value(self) -> Value {
                $destination(Some(self as _))
            }
            fn try_from_value(value: Value) -> Result<Self> {
                match value {
                    $destination(Some(v), ..) => Ok(v as _),
                    $($pat_rest => $expr_rest,)*
                    #[allow(unreachable_patterns)]
                    Value::Int32(Some(v), ..) => {
                        if (v as i128).clamp(<$source>::MIN as _, <$source>::MAX as _) != v as i128 {
                            return Err(Error::msg(format!(
                                "Value {v}: i32 is out of range for {}",
                                any::type_name::<Self>(),
                            )));
                        }
                        Ok(v as $source)
                    }
                    #[allow(unreachable_patterns)]
                    // SQL INTEGER becomes i64
                    Value::Int64(Some(v), ..) => {
                        if (v as i128).clamp(<$source>::MIN as _, <$source>::MAX as _) != v as i128 {
                            return Err(Error::msg(format!(
                                "Value {v}: i64 is out of range for {}",
                                any::type_name::<Self>(),
                            )));
                        }
                        Ok(v as $source)
                    }
                    #[allow(unreachable_patterns)]
                    // SQL VARINT becomes i128
                    Value::Int128(Some(v), ..) => {
                        if v < 0 || v > u64::MAX as _ {
                            return Err(Error::msg(format!("Value {v}: i128 does not fit into u64")));
                        }
                        Ok(v as _)
                    },
                    #[allow(unreachable_patterns)]
                    // SQL Generic floating point value
                    Value::Float64(Some(v), ..) => {
                        if v.is_finite() && v.fract() == 0.0 {
                            return Self::try_from_value(Value::Int64(Some(v as _)))
                        }
                        Err(Error::msg(format!("Value {v}: f64 does not fit into a integer")))
                    },
                    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
                        let integer = v.as_i128().or_else(|| {
                            if let Some(v) = v.as_f64() && v.fract() == 0.0 {
                                return Some(v.trunc() as i128);
                            }
                            None
                        });
                        if let Some(v) = integer
                            && v.clamp(<$source>::MIN as _, <$source>::MAX as _) == v as i128 {
                            return Ok(v as $source);
                        }
                        Err(Error::msg(format!(
                            "Value {v} from json number is out of range for {} (extracted integer is: {integer:?})",
                            any::type_name::<Self>(),
                        )))
                    }
                    // This is needed to allow integer keys in maps, in some drivers the maps are json objects
                    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
                    Value::Unknown(Some(ref v), ..) => Self::parse(v),
                    _ => Err(Error::msg(format!(
                        "Cannot convert {value:?} to {}",
                        any::type_name::<Self>(),
                    ))),
                }
            }
            fn parse(input: impl AsRef<str>) -> Result<Self> {
                input.as_ref().parse::<Self>().map_err(Into::into)
            }
        }
    };
}
impl_as_value!(
    i8,
    Value::Int8,
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Int16(Some(v), ..) => {
        let result = v as i8;
        if result as i16 != v {
            return Err(Error::msg(format!("Value {v}: i16 is out of range for i8")));
        }
        Ok(result)
    },
);
impl_as_value!(
    i16,
    Value::Int16,
    Value::Int8(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
);
impl_as_value!(
    i32,
    Value::Int32,
    Value::Int16(Some(v), ..) => Ok(v as _),
    Value::Int8(Some(v), ..) => Ok(v as _),
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into i32"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_i32().ok_or(error)
    }
);
impl_as_value!(
    i64,
    Value::Int64,
    Value::Int32(Some(v), ..) => Ok(v as _),
    Value::Int16(Some(v), ..) => Ok(v as _),
    Value::Int8(Some(v), ..) => Ok(v as _),
    Value::UInt64(Some(v), ..) => Ok(v as _),
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into i64"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_i64().ok_or(error)
    }
);
impl_as_value!(
    i128,
    Value::Int128,
    Value::Int64(Some(v), ..) => Ok(v as _),
    Value::Int32(Some(v), ..) => Ok(v as _),
    Value::Int16(Some(v), ..) => Ok(v as _),
    Value::Int8(Some(v), ..) => Ok(v as _),
    Value::UInt128(Some(v), ..) => Ok(v as _),
    Value::UInt64(Some(v), ..) => Ok(v as _),
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into i128"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_i128().ok_or(error)
    }
);
impl_as_value!(
    isize,
    Value::Int64,
    Value::Int32(Some(v), ..) => Ok(v as _),
    Value::Int16(Some(v), ..) => Ok(v as _),
    Value::Int8(Some(v), ..) => Ok(v as _),
    Value::UInt64(Some(v), ..) => Ok(v as _),
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into i64"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_isize().ok_or(error)
    }
);
impl_as_value!(
    u8,
    Value::UInt8,
    Value::Int16(Some(v), ..) => {
        v.to_u8().ok_or(Error::msg(format!("Value {v}: i16 is out of range for u8")))
    }
);
impl_as_value!(
    u16,
    Value::UInt16,
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Int32(Some(v), ..) => {
        let result = v as u16;
        if result as i32 != v {
            return Err(Error::msg(format!("Value {v}: i32 is out of range for u16")));
        }
        Ok(result)
    }
);
impl_as_value!(
    u32,
    Value::UInt32,
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
);
impl_as_value!(
    u64,
    Value::UInt64,
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into u64"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_u64().ok_or(error)
    }
);
impl_as_value!(
    u128,
    Value::UInt128,
    Value::UInt64(Some(v), ..) => Ok(v as _),
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into u128"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_u128().ok_or(error)
    }
);
impl_as_value!(
    usize,
    Value::UInt64,
    Value::UInt32(Some(v), ..) => Ok(v as _),
    Value::UInt16(Some(v), ..) => Ok(v as _),
    Value::UInt8(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => {
        let error = Error::msg(format!("Value {v}: Decimal does not fit into u64"));
        if !v.is_integer() {
            return Err(error.context("The value is not an integer"));
        }
        v.to_usize().ok_or(error)
    }
);

macro_rules! impl_as_value {
    ($source:ty, $destination:path, $extract:expr $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        impl AsValue for $source {
            fn as_empty_value() -> Value {
                $destination(None)
            }
            fn as_value(self) -> Value {
                $destination(Some(self.into()))
            }
            fn try_from_value(value: Value) -> Result<Self> {
                match value {
                    $destination(Some(v), ..) => Ok(v.into()),
                    $($pat_rest => $expr_rest,)*
                    Value::Unknown(Some(ref v), ..) => <Self as AsValue>::parse(v),
                    _ => Err(Error::msg(format!(
                        "Cannot convert {value:?} to {}",
                        any::type_name::<Self>(),
                    ))),
                }
            }
            fn parse(input: impl AsRef<str>)  -> Result<Self> {
                $extract(input.as_ref())
            }
        }
    };
}
impl_as_value!(
    bool,
    Value::Boolean,
    |input: &str| {
        match input {
            x if x.eq_ignore_ascii_case("true") || x.eq_ignore_ascii_case("t") || x.eq("1") => Ok(true),
            x if x.eq_ignore_ascii_case("false") || x.eq_ignore_ascii_case("f") || x.eq("0") => Ok(false),
            _  => return Err(Error::msg(format!("Cannot parse boolean from `{input}`")))
        }
    },
    Value::Int8(Some(v), ..) => Ok(v != 0),
    Value::Int16(Some(v), ..) => Ok(v != 0),
    Value::Int32(Some(v), ..) => Ok(v != 0),
    Value::Int64(Some(v), ..) => Ok(v != 0),
    Value::Int128(Some(v), ..) => Ok(v != 0),
    Value::UInt8(Some(v), ..) => Ok(v != 0),
    Value::UInt16(Some(v), ..) => Ok(v != 0),
    Value::UInt32(Some(v), ..) => Ok(v != 0),
    Value::UInt64(Some(v), ..) => Ok(v != 0),
    Value::UInt128(Some(v), ..) => Ok(v != 0),
    Value::Json(Some(serde_json::Value::Bool(v)), ..) => Ok(v),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let n = v.as_i64();
        if n == Some(0) {
            Ok(false)
        } else if n == Some(1) {
            Ok(true)
        } else {
            Err(Error::msg(format!("Cannot convert json number `{v:?}` into bool")))
        }
    },
);
impl_as_value!(
    f32,
    Value::Float32,
    |input: &str| Ok(input.parse::<f32>()?),
    Value::Float64(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => Ok(v.try_into()?),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let Some(v) = v.as_f64() else {
            return Err(Error::msg(format!("Cannot convert json number `{v:?}` into f32")));
        };
        Ok(v as _)
    }
);
impl_as_value!(
    f64,
    Value::Float64,
    |input: &str| Ok(input.parse::<f64>()?),
    Value::Float32(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => Ok(v.try_into()?),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let Some(v) = v.as_f64() else {
            return Err(Error::msg(format!("Cannot convert json number `{v:?}` into f32")));
        };
        Ok(v)
    }
);

impl_as_value!(
    char,
    Value::Char,
    |input: &str| {
        if input.len() != 1 {
            return Err(Error::msg(format!("Cannot convert `{input:?}` into a char")))
        }
        Ok(input.chars().next().expect("Should have one character"))
    },
    Value::Varchar(Some(v), ..) => {
        if v.len() != 1 {
            return Err(Error::msg(format!(
                "Cannot convert varchar `{}` into a char because it has more than one character",
                truncate_long!(v)
            )))
        }
        Ok(v.chars().next().unwrap())
    },
    Value::Json(Some(serde_json::Value::String(v)), ..) => {
        if v.len() != 1 {
            return Err(Error::msg(format!(
                "Cannot convert json `{}` into a char because it has more than one character",
                truncate_long!(v)
            )))
        }
        Ok(v.chars().next().unwrap())
    }
);
impl_as_value!(
    String,
    Value::Varchar,
    |input: &str| {
        Ok(input.into())
    },
    Value::Int8(Some(v), ..) => Ok(v.to_string()),
    Value::Int16(Some(v), ..) => Ok(v.to_string()),
    Value::Int32(Some(v), ..) => Ok(v.to_string()),
    Value::Int64(Some(v), ..) => Ok(v.to_string()),
    Value::Int128(Some(v), ..) => Ok(v.to_string()),
    Value::UInt8(Some(v), ..) => Ok(v.to_string()),
    Value::UInt16(Some(v), ..) => Ok(v.to_string()),
    Value::UInt32(Some(v), ..) => Ok(v.to_string()),
    Value::UInt64(Some(v), ..) => Ok(v.to_string()),
    Value::UInt128(Some(v), ..) => Ok(v.to_string()),
    Value::Float32(Some(v), ..) => Ok(v.to_string()),
    Value::Float64(Some(v), ..) => Ok(v.to_string()),
    Value::Decimal(Some(v), ..) => Ok(v.to_string()),
    Value::Char(Some(v), ..) => Ok(v.into()),
    Value::Date(Some(v), ..) => {
        let mut out = String::new();
        print_date(&mut out, "", &v);
        Ok(out)
    },
    Value::Time(Some(v), ..) => {
        let mut out = String::new();
        print_timer(&mut out, "", v.hour() as _, v.minute(), v.second(), v.nanosecond());
        Ok(out)
    },
    Value::Timestamp(Some(v), ..) => {
        let date = v.date();
        let time = v.time();
        let mut out = String::new();
        print_date(&mut out, "", &date);
        out.push(' ');
        print_timer(&mut out, "", time.hour() as _ , time.minute(), time.second(), time.nanosecond());
        Ok(out)
    },
    Value::TimestampWithTimezone(Some(v), ..) => {
        let date = v.date();
        let time = v.time();
        let mut out = String::new();
        print_date(&mut out, "", &date);
        out.push(' ');
        print_timer(&mut out, "", time.hour() as _, time.minute(), time.second(), time.nanosecond());
        let (h, m, s) = v.offset().as_hms();
        out.push(' ');
        if h >= 0 {
            out.push('+');
        } else {
            out.push('-');
        }
        let offset = match Time::from_hms(h.abs() as _, m.abs() as _, s.abs() as _){
            Ok(v) => v,
            Err(e) => return Err(Error::new(e)),
        };
        print_timer(&mut out, "", offset.hour() as _, offset.minute(), offset.second(), offset.nanosecond());
        Ok(out)
    },
    Value::Uuid(Some(v), ..) => Ok(v.to_string()),
    Value::Json(Some(serde_json::Value::String(v)), ..) => Ok(v),
);
impl_as_value!(Box<[u8]>, Value::Blob, |mut input: &str| {
    if input.starts_with("\\x") {
        input = &input[2..];
    }
    let filter_x = input.contains('x');
    let result = if filter_x {
        hex::decode(input.chars().filter(|c| *c != 'x').collect::<String>())
    } else {
        hex::decode(input)
    }
    .map(Into::into)
    .context(format!(
        "While decoding `{}` as {}",
        truncate_long!(input),
        any::type_name::<Self>()
    ))?;
    Ok(result)
});
impl_as_value!(
    Interval,
    Value::Interval,
    |mut input: &str| {
        let context = || {
            Error::msg(format!(
                "Cannot parse interval from `{}`",
                truncate_long!(input)
            ))
            .into()
        };
        match input.chars().peekable().peek() {
            Some(v) if *v == '"' || *v == '\'' => {
                input = &input[1..];
                if !input.ends_with(*v) {
                    return Err(context());
                }
                input = input.trim_end_matches(*v);
            }
            _ => {}
        };
        let mut interval = Interval::ZERO;
        loop {
            let mut cur = input;
            let Ok(count) = extract_number::<true>(&mut cur).parse::<i128>() else {
                break;
            };
            cur = cur.trim_start();
            let unit = consume_while(&mut cur, char::is_ascii_alphabetic);
            if unit.is_empty() {
                break;
            }
            match unit {
                x if x.eq_ignore_ascii_case("y")
                    || x.eq_ignore_ascii_case("year")
                    || x.eq_ignore_ascii_case("years") =>
                {
                    interval += Interval::from_years(count as _)
                }
                x if x.eq_ignore_ascii_case("mon")
                    || x.eq_ignore_ascii_case("mons")
                    || x.eq_ignore_ascii_case("month")
                    || x.eq_ignore_ascii_case("months") =>
                {
                    interval += Interval::from_months(count as _)
                }
                x if x.eq_ignore_ascii_case("d")
                    || x.eq_ignore_ascii_case("day")
                    || x.eq_ignore_ascii_case("days") =>
                {
                    interval += Interval::from_days(count as _)
                }
                x if x.eq_ignore_ascii_case("h")
                    || x.eq_ignore_ascii_case("hour")
                    || x.eq_ignore_ascii_case("hours") =>
                {
                    interval += Interval::from_hours(count as _)
                }
                x if x.eq_ignore_ascii_case("min")
                    || x.eq_ignore_ascii_case("mins")
                    || x.eq_ignore_ascii_case("minute")
                    || x.eq_ignore_ascii_case("minutes") =>
                {
                    interval += Interval::from_mins(count as _)
                }
                x if x.eq_ignore_ascii_case("s")
                    || x.eq_ignore_ascii_case("sec")
                    || x.eq_ignore_ascii_case("secs")
                    || x.eq_ignore_ascii_case("second")
                    || x.eq_ignore_ascii_case("seconds") =>
                {
                    interval += Interval::from_secs(count as _)
                }
                x if x.eq_ignore_ascii_case("micro")
                    || x.eq_ignore_ascii_case("micros")
                    || x.eq_ignore_ascii_case("microsecond")
                    || x.eq_ignore_ascii_case("microseconds") =>
                {
                    interval += Interval::from_micros(count as _)
                }
                x if x.eq_ignore_ascii_case("ns")
                    || x.eq_ignore_ascii_case("nano")
                    || x.eq_ignore_ascii_case("nanos")
                    || x.eq_ignore_ascii_case("nanosecond")
                    || x.eq_ignore_ascii_case("nanoseconds") =>
                {
                    interval += Interval::from_nanos(count as _)
                }
                _ => return Err(context()),
            }
            input = cur.trim_start();
        }
        let neg = if Some('-') == input.chars().next() {
            input = input[1..].trim_ascii_start();
            true
        } else {
            false
        };
        let mut time_interval = Interval::ZERO;
        let num = extract_number::<true>(&mut input);
        if !num.is_empty() {
            let num = num.parse::<u64>().with_context(context)?;
            time_interval += Interval::from_hours(num as _);
            if Some(':') == input.chars().next() {
                input = &input[1..];
                let num = extract_number::<false>(&mut input).parse::<u64>()?;
                if input.is_empty() {
                    return Err(context());
                }
                time_interval += Interval::from_mins(num as _);
                if Some(':') == input.chars().next() {
                    input = &input[1..];
                    let num = extract_number::<false>(&mut input)
                        .parse::<u64>()
                        .with_context(context)?;
                    time_interval += Interval::from_secs(num as _);
                    if Some('.') == input.chars().next() {
                        input = &input[1..];
                        let len = input.len();
                        let mut num = extract_number::<true>(&mut input)
                            .parse::<i128>()
                            .with_context(context)?;
                        let magnitude = (len - 1) / 3;
                        num *= 10_i128.pow(2 - (len + 2) as u32 % 3);
                        match magnitude {
                            0 => time_interval += Interval::from_millis(num),
                            1 => time_interval += Interval::from_micros(num),
                            2 => time_interval += Interval::from_nanos(num),
                            _ => return Err(context()),
                        }
                    }
                }
            }
            if neg {
                interval -= time_interval;
            } else {
                interval += time_interval;
            }
        }
        if !input.is_empty() {
            return Err(context());
        }
        Ok(interval)
    },
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);
impl_as_value!(
    std::time::Duration,
    Value::Interval,
    |v| <Interval as AsValue>::parse(v).map(Into::into),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);
impl_as_value!(
    time::Duration,
    Value::Interval,
    |v| <Interval as AsValue>::parse(v).map(Into::into),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);
impl_as_value!(
    Uuid,
    Value::Uuid,
    |input: &str| {
        let uuid = Uuid::parse_str(input).with_context(|| {
            format!(
                "Cannot parse a uuid value from `{}`",
                truncate_long!(input)
            )
        })?;
        Ok(uuid)
    },
    Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);

macro_rules! parse_time {
    ($value: ident, $($formats:literal),+ $(,)?) => {
        'value: {
            let context = || Error::msg(format!(
                "Cannot parse `{}` as {}",
                truncate_long!($value),
                any::type_name::<Self>()
            ));
            for format in [$($formats,)+] {
                let format = parse_borrowed::<2>(format)?;
                let mut parsed = time::parsing::Parsed::new();
                let remaining = parsed.parse_items($value.as_bytes(), &format);
                if let Ok(remaining) = remaining {
                    let result = parsed.try_into().with_context(context)?;
                    $value = &$value[($value.len() - remaining.len())..];
                    break 'value Ok(result);
                }
            }
            Err(context())
        }
    }
}

impl_as_value!(
    time::Date,
    Value::Date,
    |input: &str| {
        let mut value = input;
        let mut result: time::Date = parse_time!(value, "[year]-[month]-[day]")?;
        {
            let mut attempt = value.trim_start();
            let suffix = consume_while(&mut attempt, char::is_ascii_alphabetic);
            if suffix.eq_ignore_ascii_case("bc") {
                result =
                    time::Date::from_calendar_date(-(result.year() - 1), result.month(), result.day())?;
                value = attempt;
            }
            if suffix.eq_ignore_ascii_case("ad") {
                value = attempt
            }
        }
        if !value.is_empty() {
            return Err(Error::msg(format!("Cannot parse `{}` as time::Date", truncate_long!(input))))
        }
        Ok(result)
    },
    Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);
impl_as_value!(
    time::Time,
    Value::Time,
    |mut input: &str| {
        let result: time::Time = parse_time!(
            input,
            "[hour]:[minute]:[second].[subsecond]",
            "[hour]:[minute]:[second]",
            "[hour]:[minute]",
        )?;
        if !input.is_empty() {
            return Err(Error::msg(format!("Cannot parse `{}` as time::Time", truncate_long!(input))))
        }
        Ok(result)
    },
    Value::Interval(Some(v), ..) => {
        let (h, m, s, ns) = v.as_hmsns();
        time::Time::from_hms_nano(h as _, m, s, ns,)
            .map_err(|e| Error::msg(format!("Cannot convert interval `{v:?}` into time: {e:?}")))
    },
    Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);

impl_as_value!(
    time::PrimitiveDateTime,
    Value::Timestamp,
    |mut input: &str| {
        let result: time::PrimitiveDateTime = parse_time!(
            input,
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]",
            "[year]-[month]-[day]T[hour]:[minute]:[second]",
            "[year]-[month]-[day]T[hour]:[minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]",
            "[year]-[month]-[day] [hour]:[minute]:[second]",
            "[year]-[month]-[day] [hour]:[minute]",
        )?;
        if !input.is_empty() {
            return Err(Error::msg(format!("Cannot parse `{}` as time::PrimitiveDateTime", truncate_long!(input))))
        }
        Ok(result)
    },
    Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);

impl_as_value!(
    time::OffsetDateTime,
    Value::TimestampWithTimezone,
    |mut input: &str| {
        if let Ok::<time::OffsetDateTime, _>(result) = parse_time!(
            input,
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]",
            "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]",
            "[year]-[month]-[day]T[hour]:[minute][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute][offset_hour sign:mandatory]",
        ) {
            return Ok(result);
        }
        if let Ok(result) = <PrimitiveDateTime as AsValue>::parse(input).map(|v| v.assume_utc()) {
            return Ok(result);
        }
        Err(Error::msg(format!("Cannot parse `{}` as time::OffsetDateTime", truncate_long!(input))))
    },
    Value::Timestamp(Some(timestamp), ..) => Ok(timestamp.assume_utc()),
    Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
    Value::Json(Some(serde_json::Value::String(ref v)), ..) => <Self as AsValue>::parse(v),
);

#[cfg(feature = "chrono")]
impl AsValue for chrono::NaiveDate {
    fn as_empty_value() -> Value {
        Value::Date(None)
    }
    fn as_value(self) -> Value {
        Value::Date(
            'date: {
                time::Date::from_calendar_date(
                    self.year(),
                    number_to_month!(
                        self.month(),
                        break 'date Err(Error::msg(format!(
                            "Unexpected month value {}",
                            self.month()
                        )))
                    ),
                    self.day() as _,
                )
                .map_err(Into::into)
            }
            .inspect_err(|e| {
                log::error!("Could not create a Value::Date from chrono::NaiveDate: {e:?}");
            })
            .ok(),
        )
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!(
            "Could not create a chrono::NaiveDate from {value:?}"
        ));
        let v = <time::Date as AsValue>::try_from_value(value).context(context.clone())?;
        chrono::NaiveDate::from_ymd_opt(v.year(), month_to_number!(v.month()), v.day() as _)
            .context(context)
    }
}

#[cfg(feature = "chrono")]
impl AsValue for chrono::NaiveTime {
    fn as_empty_value() -> Value {
        Value::Time(None)
    }
    fn as_value(self) -> Value {
        Value::Time(
            time::Time::from_hms_nano(
                self.hour() as _,
                self.minute() as _,
                self.second() as _,
                self.nanosecond() as _,
            )
            .inspect_err(|e| {
                log::error!("Could not create a Value::Time from chrono::NaiveTime: {e:?}",)
            })
            .ok(),
        )
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!(
            "Could not create a chrono::NaiveTime from {value:?}"
        ));
        let v = <time::Time as AsValue>::try_from_value(value).context(context.clone())?;
        Self::from_hms_nano_opt(
            v.hour() as _,
            v.minute() as _,
            v.second() as _,
            v.nanosecond() as _,
        )
        .context(context)
    }
}

#[cfg(feature = "chrono")]
impl AsValue for chrono::NaiveDateTime {
    fn as_empty_value() -> Value {
        Value::Timestamp(None)
    }
    fn as_value(self) -> Value {
        Value::Timestamp(
            'value: {
                let Ok(date) = AsValue::try_from_value(self.date().as_value()) else {
                    break 'value Err(Error::msg(
                        "failed to convert the date part from chrono::NaiveDate to time::Date",
                    ));
                };
                let Ok(time) = AsValue::try_from_value(self.time().as_value()) else {
                    break 'value Err(Error::msg(
                        "failed to convert the time part from chrono::NaiveTime to time::Time",
                    ));
                };
                Ok(time::PrimitiveDateTime::new(date, time))
            }
            .inspect_err(|e| {
                log::error!(
                    "Could not create a Value::Timestamp from chrono::NaiveDateTime: {e:?}",
                );
            })
            .ok(),
        )
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!(
            "Could not create a chrono::NaiveDateTime from {value:?}"
        ));
        let v =
            <time::PrimitiveDateTime as AsValue>::try_from_value(value).context(context.clone())?;
        let date = AsValue::try_from_value(v.date().as_value()).context(context.clone())?;
        let time = AsValue::try_from_value(v.time().as_value()).context(context.clone())?;
        Ok(Self::new(date, time))
    }
}

#[cfg(feature = "chrono")]
impl AsValue for chrono::DateTime<chrono::FixedOffset> {
    fn as_empty_value() -> Value {
        Value::TimestampWithTimezone(None)
    }
    fn as_value(self) -> Value {
        Value::TimestampWithTimezone(
            'value: {
                use chrono::Offset;
                let Ok(date) = AsValue::try_from_value(self.date_naive().as_value()) else {
                    break 'value Err(Error::msg(
                        "failed to convert the date part from chrono::NaiveDate to time::Date",
                    ));
                };
                let Ok(time) = AsValue::try_from_value(self.time().as_value()) else {
                    break 'value Err(Error::msg(
                        "failed to convert the time part from chrono::NaiveTime to time::Time",
                    ));
                };
                let Ok(offset) =
                    time::UtcOffset::from_whole_seconds(self.offset().fix().local_minus_utc())
                else {
                    break 'value Err(Error::msg("failed to convert the offset part from"));
                };
                Ok(time::OffsetDateTime::new_in_offset(date, time, offset))
            }
            .inspect_err(|e| {
                log::error!(
                    "Could not create a Value::Timestamp from chrono::NaiveDateTime: {e:?}",
                );
            })
            .ok(),
        )
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!(
            "Could not create a chrono::DateTime from {value:?}"
        ));
        let v =
            <time::OffsetDateTime as AsValue>::try_from_value(value).context(context.clone())?;
        let date = AsValue::try_from_value(v.date().as_value()).context(context.clone())?;
        let time = AsValue::try_from_value(v.time().as_value()).context(context.clone())?;
        let date_time = chrono::NaiveDateTime::new(date, time);
        let offset =
            chrono::FixedOffset::east_opt(v.offset().whole_seconds()).context(context.clone())?;
        Ok(Self::from_naive_utc_and_offset(date_time, offset))
    }
}

#[cfg(feature = "chrono")]
impl AsValue for chrono::DateTime<chrono::Utc> {
    fn as_empty_value() -> Value {
        Value::TimestampWithTimezone(None)
    }
    fn as_value(self) -> Value {
        let odt = time::OffsetDateTime::from_unix_timestamp_nanos(
            self.timestamp_nanos_opt().unwrap() as i128,
        )
        .unwrap();
        Value::TimestampWithTimezone(Some(odt))
    }
    fn try_from_value(value: Value) -> Result<Self> {
        let odt = <time::OffsetDateTime as AsValue>::try_from_value(value)?;
        let utc_odt = odt.to_offset(time::UtcOffset::UTC);
        let secs = utc_odt.unix_timestamp();
        let nanos = utc_odt.nanosecond();
        Self::from_timestamp(secs, nanos)
            .ok_or_else(|| Error::msg("Timestamp out of range for chrono::DateTime<Utc>"))
    }
}

impl AsValue for Decimal {
    fn as_empty_value() -> Value {
        Value::Decimal(None, 0, 0)
    }
    fn as_value(self) -> Value {
        Value::Decimal(Some(self), 0, self.scale() as _)
    }
    fn try_from_value(value: Value) -> Result<Self> {
        match value {
            Value::Decimal(Some(v), ..) => Ok(v),
            Value::Int8(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int16(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int32(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int64(Some(v), ..) => Ok(Decimal::new(v, 0)),
            Value::UInt8(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt16(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt32(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt64(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Float32(Some(v), ..) => Ok(Decimal::from_f32(v)
                .ok_or(Error::msg(format!("Cannot convert {value:?} to Decimal")))?),
            Value::Float64(Some(v), ..) => Ok(Decimal::from_f64(v)
                .ok_or(Error::msg(format!("Cannot convert {value:?} to Decimal")))?),
            Value::Json(Some(serde_json::Value::Number(v)), ..) => {
                if let Some(v) = v.as_f64()
                    && let Some(v) = Decimal::from_f64(v)
                {
                    return Ok(v);
                }
                Err(Error::msg(format!(
                    "Value {v} from json number is out of range for Decimal",
                )))
            }
            Value::Unknown(Some(v), ..) => Self::parse(&v),
            _ => Err(Error::msg(format!("Cannot convert {value:?} to Decimal"))),
        }
    }
    fn parse(input: impl AsRef<str>) -> Result<Self> {
        let input = input.as_ref();
        Ok(input.parse::<Decimal>().with_context(|| {
            Error::msg(format!(
                "Cannot parse a decimal value from `{}`",
                truncate_long!(input)
            ))
        })?)
    }
}

impl<const W: u8, const S: u8> AsValue for FixedDecimal<W, S> {
    fn as_empty_value() -> Value {
        Decimal::as_empty_value()
    }
    fn as_value(self) -> Value {
        Value::Decimal(Some(self.0), W, self.0.scale() as _)
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(Decimal::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self> {
        <Decimal as AsValue>::parse(input).map(Into::into)
    }
}

impl<T: AsValue, const N: usize> AsValue for [T; N] {
    fn as_empty_value() -> Value {
        Value::Array(None, Box::new(T::as_empty_value()), N as u32)
    }
    fn as_value(self) -> Value {
        Value::Array(
            Some(self.into_iter().map(AsValue::as_value).collect()),
            Box::new(T::as_empty_value()),
            N as u32,
        )
    }
    fn try_from_value(value: Value) -> Result<Self> {
        fn convert_iter<T: AsValue, const N: usize>(
            iter: impl IntoIterator<Item: AsValue>,
        ) -> Result<[T; N]> {
            iter.into_iter()
                .map(|v| T::try_from_value(v.as_value()))
                .collect::<Result<Vec<_>>>()?
                .try_into()
                .map_err(|v: Vec<T>| {
                    Error::msg(format!(
                        "Expected array of length {N}, got {} elements ({})",
                        v.len(),
                        any::type_name::<[T; N]>()
                    ))
                })
        }
        match value {
            Value::Varchar(Some(v), ..)
                if matches!(T::as_empty_value(), Value::Char(..)) && v.len() == N =>
            {
                convert_iter(v.chars())
            }
            Value::List(Some(v), ..) if v.len() == N => convert_iter(v.into_iter()),
            Value::Array(Some(v), ..) if v.len() == N => convert_iter(v.into_iter()),
            Value::Json(Some(serde_json::Value::Array(v))) if v.len() == N => {
                convert_iter(v.into_iter())
            }
            Value::Unknown(Some(v)) => <Self as AsValue>::parse(v),
            _ => Err(Error::msg(format!(
                "Cannot convert {value:?} to array {}",
                any::type_name::<Self>()
            ))),
        }
    }
}

macro_rules! impl_as_value {
    ($source:ident) => {
        impl<T: AsValue> AsValue for $source<T> {
            fn as_empty_value() -> Value {
                Value::List(None, Box::new(T::as_empty_value()))
            }
            fn as_value(self) -> Value {
                Value::List(
                    Some(self.into_iter().map(AsValue::as_value).collect()),
                    Box::new(T::as_empty_value()),
                )
            }
            fn try_from_value(value: Value) -> Result<Self> {
                match value {
                    Value::List(Some(v), ..) => Ok(v
                        .into_iter()
                        .map(|v| Ok::<_, Error>(<T as AsValue>::try_from_value(v)?))
                        .collect::<Result<_>>()?),
                    Value::List(None, ..) => Ok($source::<T>::new()),
                    Value::Array(Some(v), ..) => Ok(v
                        .into_iter()
                        .map(|v| Ok::<_, Error>(<T as AsValue>::try_from_value(v)?))
                        .collect::<Result<_>>()?),
                    Value::Json(Some(serde_json::Value::Array(v)), ..) => Ok(v
                        .into_iter()
                        .map(|v| Ok::<_, Error>(<T as AsValue>::try_from_value(v.as_value())?))
                        .collect::<Result<_>>()?),
                    _ => Err(Error::msg(format!(
                        "Cannot convert {value:?} to {}",
                        any::type_name::<Self>(),
                    ))),
                }
            }
        }
    };
}
impl_as_value!(Vec);
impl_as_value!(VecDeque);
impl_as_value!(LinkedList);

macro_rules! impl_as_value {
    ($source:ident, $($key_trait:ident),+) => {
        impl<K: AsValue $(+ $key_trait)+, V: AsValue> AsValue for $source<K, V> {
            fn as_empty_value() -> Value {
                Value::Map(None, K::as_empty_value().into(), V::as_empty_value().into())
            }
            fn as_value(self) -> Value {
                Value::Map(
                    Some(
                        self.into_iter()
                            .map(|(k, v)| (k.as_value(), v.as_value()))
                            .collect(),
                    ),
                    K::as_empty_value().into(),
                    V::as_empty_value().into(),
                )
            }
            fn try_from_value(value: Value) -> Result<Self> {
                match value {
                    Value::Map(Some(v), ..) => {
                        Ok(v.into_iter()
                            .map(|(k, v)| {
                                Ok((
                                    <K as AsValue>::try_from_value(k)?,
                                    <V as AsValue>::try_from_value(v)?,
                                ))
                            })
                            .collect::<Result<_>>()?)
                    }
                    Value::Json(Some(serde_json::Value::Object(v)), ..) => {
                        Ok(v.into_iter()
                            .map(|(k, v)| {
                                Ok((
                                    <K as AsValue>::try_from_value(serde_json::Value::String(k).as_value())?,
                                    <V as AsValue>::try_from_value(v.as_value())?,
                                ))
                            })
                            .collect::<Result<_>>()?)
                    }
                    _=> {
                        Err(Error::msg(format!(
                            "Cannot convert {value:?} to {}",
                            any::type_name::<Self>(),
                        )))
                    }
                }
            }
        }
    }
}
impl_as_value!(BTreeMap, Ord);
impl_as_value!(HashMap, Eq, Hash);

impl AsValue for &'static str {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        Value::Varchar(Some(self.into()))
    }
    fn try_from_value(_value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        Err(Error::msg(
            "Cannot get a string reference from a owned value",
        ))
    }
}

impl AsValue for Cow<'static, str> {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        Value::Varchar(Some(self.into()))
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        String::try_from_value(value).map(Into::into)
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        <String as AsValue>::parse(input).map(Into::into)
    }
}

impl<T: AsValue> AsValue for Passive<T> {
    fn as_empty_value() -> Value {
        T::as_empty_value()
    }
    fn as_value(self) -> Value {
        match self {
            Passive::Set(v) => v.as_value(),
            Passive::NotSet => T::as_empty_value(),
        }
    }
    fn try_from_value(value: Value) -> Result<Self> {
        Ok(Passive::Set(<T as AsValue>::try_from_value(value)?))
    }
}

impl<T: AsValue> AsValue for Option<T> {
    fn as_empty_value() -> Value {
        T::as_empty_value()
    }
    fn as_value(self) -> Value {
        match self {
            Some(v) => v.as_value(),
            None => T::as_empty_value(),
        }
    }
    fn try_from_value(value: Value) -> Result<Self> {
        Ok(if value.is_null() {
            None
        } else {
            Some(<T as AsValue>::try_from_value(value)?)
        })
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        let mut value = input.as_ref();
        let result = consume_while(&mut value, |v| v.is_alphanumeric() || *v == '_');
        if result.eq_ignore_ascii_case("null") {
            return Ok(None);
        };
        T::parse(input).map(Some)
    }
}

// TODO: Use the macro below once box_into_inner is stabilized
impl<T: AsValue> AsValue for Box<T> {
    fn as_empty_value() -> Value {
        T::as_empty_value()
    }
    fn as_value(self) -> Value {
        (*self).as_value()
    }
    fn try_from_value(value: Value) -> Result<Self> {
        Ok(Self::new(<T as AsValue>::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        T::parse(input).map(Self::new)
    }
}

macro_rules! impl_as_value {
    ($source:ident) => {
        impl<T: AsValue + ToOwned<Owned = impl AsValue>> AsValue for $source<T> {
            fn as_empty_value() -> Value {
                T::as_empty_value()
            }
            fn as_value(self) -> Value {
                $source::<T>::into_inner(self).as_value()
            }
            fn try_from_value(value: Value) -> Result<Self> {
                Ok($source::new(<T as AsValue>::try_from_value(value)?))
            }
        }
    };
}
// impl_as_value!(Box);
impl_as_value!(Cell);
impl_as_value!(RefCell);

impl<T: AsValue> AsValue for RwLock<T> {
    fn as_empty_value() -> Value {
        T::as_empty_value()
    }
    fn as_value(self) -> Value {
        self.into_inner()
            .expect("Error occurred while trying to take the content of the RwLock")
            .as_value()
    }
    fn try_from_value(value: Value) -> Result<Self> {
        Ok(RwLock::new(<T as AsValue>::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        T::parse(input).map(Self::new)
    }
}

macro_rules! impl_as_value {
    ($source:ident) => {
        impl<T: AsValue + ToOwned<Owned = impl AsValue>> AsValue for $source<T> {
            fn as_empty_value() -> Value {
                T::as_empty_value()
            }
            fn as_value(self) -> Value {
                $source::try_unwrap(self)
                    .map(|v| v.as_value())
                    .unwrap_or_else(|v| v.as_ref().to_owned().as_value())
            }
            fn try_from_value(value: Value) -> Result<Self> {
                Ok($source::new(<T as AsValue>::try_from_value(value)?))
            }
            fn parse(input: impl AsRef<str>) -> Result<Self>
            where
                Self: Sized,
            {
                T::parse(input).map(Self::new)
            }
        }
    };
}
impl_as_value!(Arc);
impl_as_value!(Rc);

impl AsValue for serde_json::Value {
    fn as_empty_value() -> Value {
        Value::Json(None)
    }
    fn as_value(self) -> Value {
        Value::Json(Some(self))
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(if let Value::Json(v) = value {
            match v {
                Some(v) => v,
                None => Self::Null,
            }
        } else {
            return Err(Error::msg(
                "Cannot convert non json tank::Value to serde_json::Value",
            ));
        })
    }
}
