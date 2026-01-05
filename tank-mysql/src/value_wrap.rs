use std::borrow::Cow;
use tank_core::Interval;
use time::{Date, Duration, Month, PrimitiveDateTime, Time};

pub(crate) struct ValueWrap(pub(crate) tank_core::Value);

impl From<tank_core::Value> for ValueWrap {
    fn from(value: tank_core::Value) -> Self {
        Self(value)
    }
}
impl From<ValueWrap> for tank_core::Value {
    fn from(value: ValueWrap) -> Self {
        value.0
    }
}

impl mysql_async::prelude::FromValue for ValueWrap {
    type Intermediate = ValueWrap;
}

impl TryFrom<mysql_async::Value> for ValueWrap {
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

impl TryFrom<ValueWrap> for mysql_async::Value {
    type Error = tank_core::Error;

    fn try_from(value: ValueWrap) -> Result<Self, Self::Error> {
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
                    Err(Self::Error::msg(format!(
                        "Date {} is out of range for MySQL",
                        $date
                    )))
                }
            }};
        }
        Ok(match value.0 {
            _ if value.0.is_null() => MySQLValue::NULL,
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
                let days = (secs / Interval::SECS_IN_DAY.abs()) as _;
                secs = secs % Interval::SECS_IN_DAY;
                let hours = (secs / 3600).abs() as _;
                secs = secs % 3600;
                let mins = (secs / 60).abs();
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
            TankValue::Uuid(Some(v), ..) => MySQLValue::from(v),
            // TankValue::Array(Some(v), ..) => MySQLValue::from(v),
            // TankValue::List(Some(v), ..) => MySQLValue::from(v),
            // TankValue::Map(Some(v), ..) => MySQLValue::from(v),
            // TankValue::Struct(Some(v), ..) => MySQLValue::from(v),
            TankValue::Unknown(Some(v), ..) => MySQLValue::from(v),
            _ => {
                return Err(tank_core::Error::msg(format!(
                    "tank::Value variant `{:?}` is not supported by MySQL",
                    value.0
                ))
                .into());
            }
        })
    }
}
