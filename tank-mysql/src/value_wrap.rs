use anyhow::anyhow;
use mysql_async::{
    Column, Value as MyValue,
    consts::{ColumnFlags, ColumnType},
};
use rust_decimal::Decimal;
use std::borrow::Cow;
use tank_core::{AsValue, Interval};
use time::{Date, Duration, Month, PrimitiveDateTime, Time};

pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, tank_core::Value>);

/// MySQL's binary pseudo-charset, reported for binary columns.
const BINARY_CHARSET: u16 = 63;

/// Decode a raw MySQL value, using the column metadata to disambiguate it.
/// Both protocols report `DECIMAL`, `VARCHAR`, `BLOB`, `ENUM` and `JSON` as bytes.
pub(crate) fn extract_value(
    column: &Column,
    value: MyValue,
) -> tank_core::Result<ValueWrap<'static>> {
    let MyValue::Bytes(bytes) = &value else {
        // The binary protocol already reports these as typed values.
        return ValueWrap::try_from(value)
            .map_err(|_| anyhow!("Could not convert the MySQL value into a tank value"));
    };
    let text = String::from_utf8_lossy(bytes);
    let text = text.as_ref();
    let unsigned = column.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    Ok(match column.column_type() {
        ColumnType::MYSQL_TYPE_NULL => tank_core::Value::Null,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            tank_core::Value::Decimal(Some(<Decimal as AsValue>::parse(text)?), 0, 0)
        }
        ColumnType::MYSQL_TYPE_TINY => {
            if unsigned {
                tank_core::Value::UInt8(Some(<u8 as AsValue>::parse(text)?))
            } else {
                tank_core::Value::Int8(Some(<i8 as AsValue>::parse(text)?))
            }
        }
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
            if unsigned {
                tank_core::Value::UInt16(Some(<u16 as AsValue>::parse(text)?))
            } else {
                tank_core::Value::Int16(Some(<i16 as AsValue>::parse(text)?))
            }
        }
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            if unsigned {
                tank_core::Value::UInt32(Some(<u32 as AsValue>::parse(text)?))
            } else {
                tank_core::Value::Int32(Some(<i32 as AsValue>::parse(text)?))
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if unsigned {
                tank_core::Value::UInt64(Some(<u64 as AsValue>::parse(text)?))
            } else {
                tank_core::Value::Int64(Some(<i64 as AsValue>::parse(text)?))
            }
        }
        ColumnType::MYSQL_TYPE_FLOAT => {
            tank_core::Value::Float32(Some(<f32 as AsValue>::parse(text)?))
        }
        ColumnType::MYSQL_TYPE_DOUBLE => {
            tank_core::Value::Float64(Some(<f64 as AsValue>::parse(text)?))
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            tank_core::Value::Date(Some(<Date as AsValue>::parse(text)?))
        }
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            tank_core::Value::Timestamp(Some(<PrimitiveDateTime as AsValue>::parse(text)?))
        }
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            tank_core::Value::Interval(Some(parse_mysql_time(text)?))
        }
        ColumnType::MYSQL_TYPE_JSON => {
            tank_core::Value::Json(Some(serde_json::from_str(text).map_err(|e| {
                anyhow!(
                    "Could not decode the JSON column `{}`: {e}",
                    column.name_str()
                )
            })?))
        }
        // Binary payloads are identified by charset, not by `BINARY_FLAG` (MariaDB sets it on textual `UUID`).
        ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_VECTOR
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
            if column.character_set() == BINARY_CHARSET =>
        {
            tank_core::Value::Blob(Some(bytes.clone().into()))
        }
        // Text payloads (`CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET`, ...).
        _ => {
            // MariaDB reports `JSON` as a text blob, so structural JSON is recognized from the content.
            match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(json @ (serde_json::Value::Array(..) | serde_json::Value::Object(..))) => {
                    tank_core::Value::Json(Some(json))
                }
                _ => tank_core::Value::Varchar(Some(text.to_owned().into())),
            }
        }
    }
    .into())
}

/// Parse a MySQL `TIME` literal (`[-]HH:MM:SS[.ffffff]`) into an interval.
fn parse_mysql_time(text: &str) -> tank_core::Result<Interval> {
    let context = || anyhow!("Could not parse the MySQL TIME value `{text}`");
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let mut parts = text.split(':');
    let hours: i64 = parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(context)?;
    let minutes: i64 = parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(context)?;
    let seconds = parts.next().ok_or_else(context)?;
    let (seconds, micros) = match seconds.split_once('.') {
        Some((seconds, fraction)) => {
            let mut fraction = fraction.to_owned();
            fraction.truncate(6);
            while fraction.len() < 6 {
                fraction.push('0');
            }
            (
                seconds.parse::<i64>().map_err(|_| context())?,
                fraction.parse::<i64>().unwrap_or(0),
            )
        }
        None => (seconds.parse::<i64>().map_err(|_| context())?, 0),
    };
    let result = Interval::from_hours(hours)
        + Interval::from_mins(minutes)
        + Interval::from_secs(seconds)
        + Interval::from_micros(micros as i128);
    Ok(if negative { -result } else { result })
}

impl<'a> From<&'a tank_core::Value> for ValueWrap<'a> {
    fn from(value: &'a tank_core::Value) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl<'a> From<tank_core::Value> for ValueWrap<'a> {
    fn from(value: tank_core::Value) -> Self {
        Self(Cow::Owned(value))
    }
}

impl<'a> mysql_async::prelude::FromValue for ValueWrap<'a> {
    type Intermediate = ValueWrap<'a>;
}

impl<'a> TryFrom<mysql_async::Value> for ValueWrap<'a> {
    type Error = mysql_async::FromValueError;
    fn try_from(value: mysql_async::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            mysql_async::Value::NULL => tank_core::Value::Null,
            mysql_async::Value::Bytes(v) => {
                let json = serde_json::from_slice::<serde_json::Value>(&v);
                match json {
                    Ok(json @ serde_json::Value::Array(..))
                    | Ok(json @ serde_json::Value::Object(..))
                    | Ok(json @ serde_json::Value::Number(..)) => {
                        tank_core::Value::Json(Some(json))
                    }
                    _ => {
                        // TODO Replace it with String::from_utf8_lossy_owned once https://github.com/rust-lang/rust/issues/129436 is fixed
                        tank_core::Value::Unknown(Some(String::from_utf8_lossy(&v).to_string()))
                    }
                }
            }
            mysql_async::Value::Int(v) => tank_core::Value::Int64(Some(v)),
            mysql_async::Value::UInt(v) => tank_core::Value::UInt64(Some(v)),
            mysql_async::Value::Float(v) => tank_core::Value::Float32(Some(v)),
            mysql_async::Value::Double(v) => tank_core::Value::Float64(Some(v)),
            mysql_async::Value::Date(year, month, day, hour, minute, second, microsecond) => {
                tank_core::Value::Timestamp(Some(PrimitiveDateTime::new(
                    Date::from_calendar_date(
                        year as _,
                        match month {
                            1 => Month::January,
                            2 => Month::February,
                            3 => Month::March,
                            4 => Month::April,
                            5 => Month::May,
                            6 => Month::June,
                            7 => Month::July,
                            8 => Month::August,
                            9 => Month::September,
                            10 => Month::October,
                            11 => Month::November,
                            12 => Month::December,
                            _ => return Err(mysql_async::FromValueError(value)),
                        },
                        day,
                    )
                    .map_err(|_| mysql_async::FromValueError(value.clone()))?,
                    Time::from_hms_micro(hour, minute, second, microsecond)
                        .map_err(|_| mysql_async::FromValueError(value))?,
                )))
            }
            mysql_async::Value::Time(negative, days, hours, minutes, seconds, micro) => {
                tank_core::Value::Interval(Some({
                    let mut result = Interval::from_days(days as _)
                        + Interval::from_hours(hours as _)
                        + Interval::from_mins(minutes as _)
                        + Interval::from_secs(seconds as _)
                        + Interval::from_micros(micro as _);
                    if negative {
                        result = -result;
                    }
                    result
                }))
            }
        }
        .into())
    }
}

impl<'a> TryFrom<ValueWrap<'a>> for mysql_async::Value {
    type Error = tank_core::Error;

    fn try_from(value: ValueWrap<'a>) -> Result<Self, Self::Error> {
        type TankValue = tank_core::Value;
        type MySQLValue = mysql_async::Value;
        macro_rules! ensure_date_range {
            ($date:expr, $target:ty) => {{
                let year = $date.year();
                if year == year.clamp(<$target>::MIN as _, <$target>::MAX as _) {
                    Ok(MySQLValue::Date(
                        $date.year() as _,
                        $date.month().into(),
                        $date.day(),
                        $date.hour(),
                        $date.minute(),
                        $date.second(),
                        $date.microsecond(),
                    ))
                } else {
                    Err(anyhow!("Date {} is out of range for MySQL", $date))
                }
            }};
        }
        Ok(match value.0.into_owned() {
            v if v.is_null() => MySQLValue::NULL,
            TankValue::Boolean(Some(v)) => MySQLValue::from(v),
            TankValue::Int8(Some(v), ..) => MySQLValue::from(v),
            TankValue::Int16(Some(v), ..) => MySQLValue::from(v),
            TankValue::Int32(Some(v), ..) => MySQLValue::from(v),
            TankValue::Int64(Some(v), ..) => MySQLValue::from(v),
            TankValue::Int128(Some(v), ..) => MySQLValue::from(v),
            TankValue::UInt8(Some(v), ..) => MySQLValue::from(v),
            TankValue::UInt16(Some(v), ..) => MySQLValue::from(v),
            TankValue::UInt32(Some(v), ..) => MySQLValue::from(v),
            TankValue::UInt64(Some(v), ..) => MySQLValue::from(v),
            TankValue::UInt128(Some(v), ..) => MySQLValue::from(v),
            TankValue::Float32(Some(v), ..) => MySQLValue::from(v),
            TankValue::Float64(Some(v), ..) => MySQLValue::from(v),
            TankValue::Decimal(Some(v), ..) => MySQLValue::from(v),
            TankValue::Char(Some(v), ..) => MySQLValue::from(v.to_string()),
            TankValue::Varchar(Some(v), ..) => match v {
                Cow::Borrowed(v) => MySQLValue::from(v),
                Cow::Owned(v) => MySQLValue::from(v),
            },
            TankValue::Blob(Some(v), ..) => MySQLValue::from(v),
            TankValue::Date(Some(v), ..) => MySQLValue::from(v),
            TankValue::Time(Some(v), ..) => MySQLValue::from(v),
            TankValue::Timestamp(Some(v), ..) => ensure_date_range!(v, u16)?,
            TankValue::TimestampWithTimezone(Some(v), ..) => {
                let date_time = v.to_utc();
                ensure_date_range!(date_time, u16)?
            }
            TankValue::Interval(Some(v), ..) => {
                let v: Duration = v.into();
                let mut secs = v.whole_seconds();
                let days = (secs / Interval::SECS_IN_DAY).abs() as _;
                secs = (secs % Interval::SECS_IN_DAY).abs();
                let hours = (secs / 3600) as _;
                secs = secs % 3600;
                let mins = secs / 60;
                secs = secs % 60;
                MySQLValue::Time(
                    v < Duration::ZERO,
                    days,
                    hours,
                    mins as _,
                    secs as _,
                    v.subsec_microseconds().abs() as _,
                )
            }
            TankValue::Uuid(Some(v), ..) => MySQLValue::from(v.to_string()),
            // TankValue::Array(Some(v), ..) => MySQLValue::from(v),
            // TankValue::List(Some(v), ..) => MySQLValue::from(v),
            // TankValue::Map(Some(v), ..) => MySQLValue::from(v),
            // TankValue::Struct(Some(v), ..) => MySQLValue::from(v),
            TankValue::Unknown(Some(v), ..) => MySQLValue::from(v),
            v => {
                return Err(
                    anyhow!("tank::Value variant `{v:?}` is not supported by MySQL").into(),
                );
            }
        })
    }
}
