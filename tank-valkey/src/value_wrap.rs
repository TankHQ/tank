use redis::{FromRedisValue, RedisWrite, ToRedisArgs};
use std::{borrow::Cow, collections::HashMap};

#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, tank_core::Value>);

impl<'a> From<ValueWrap<'a>> for tank_core::Value {
    fn from(value: ValueWrap<'a>) -> Self {
        value.0.into_owned()
    }
}

impl<'a> From<&'a ValueWrap<'a>> for &'a tank_core::Value {
    fn from(value: &'a ValueWrap<'a>) -> Self {
        match &value.0 {
            Cow::Borrowed(v) => *v,
            Cow::Owned(v) => v,
        }
    }
}

impl<'a> From<tank_core::Value> for ValueWrap<'a> {
    fn from(value: tank_core::Value) -> Self {
        Self(Cow::Owned(value))
    }
}

impl<'a> From<&'a tank_core::Value> for ValueWrap<'a> {
    fn from(value: &'a tank_core::Value) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl<'a> TryFrom<redis::Value> for ValueWrap<'a> {
    type Error = tank_core::Error;

    fn try_from(value: redis::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            redis::Value::Nil => tank_core::Value::Null,
            redis::Value::Boolean(v) => tank_core::Value::Boolean(Some(v)),
            redis::Value::Int(v) => tank_core::Value::Int64(Some(v)),
            redis::Value::Double(v) => tank_core::Value::Float64(Some(v)),
            redis::Value::SimpleString(v) => tank_core::Value::Varchar(Some(v.into())),
            redis::Value::BulkString(items) => {
                tank_core::Value::Varchar(Some(String::from_utf8(items)?.into()))
            }
            redis::Value::Array(v) | redis::Value::Set(v) => {
                let mut it = v
                    .into_iter()
                    .map(|v| Ok::<_, tank_core::Error>(ValueWrap::try_from(v)?.into()));
                let first = it.next().transpose()?;
                let ty = first
                    .as_ref()
                    .map(tank_core::Value::as_null)
                    .unwrap_or(tank_core::Value::Null);
                let mut it = first.map(|v| Ok(v)).into_iter().chain(it).peekable();
                tank_core::Value::List(
                    if it.peek().is_some() {
                        Some(it.collect::<Result<Vec<_>, _>>()?)
                    } else {
                        None
                    },
                    Box::new(ty),
                )
            }
            redis::Value::Map(v) => {
                let mut it = v
                    .into_iter()
                    .map(|(k, v)| {
                        Ok::<_, tank_core::Error>((
                            ValueWrap::try_from(k)?.0.into_owned(),
                            ValueWrap::try_from(v)?.0.into_owned(),
                        ))
                    })
                    .peekable();
                let first = it.next().transpose()?;
                let (k_ty, v_ty) = first
                    .as_ref()
                    .map(|(k, v)| (k.as_null(), v.as_null()))
                    .unwrap_or((tank_core::Value::Null, tank_core::Value::Null));
                let mut it = first.map(|v| Ok(v)).into_iter().chain(it).peekable();
                tank_core::Value::Map(
                    if it.peek().is_some() {
                        Some(it.collect::<Result<HashMap<_, _>, _>>()?)
                    } else {
                        None
                    },
                    Box::new(k_ty),
                    Box::new(v_ty),
                )
            }
            redis::Value::Attribute { data, .. } => ValueWrap::from_redis_value(*data)?.into(),
            redis::Value::VerbatimString { text, .. } => {
                tank_core::Value::Varchar(Some(text.into()))
            }
            redis::Value::BigNumber(v) => tank_core::Value::Varchar(Some(v.to_string().into())),
            v => {
                return Err(Self::Error::msg(format!(
                    "Unexpected Valkey/Redis value {v:?}"
                )));
            }
        }
        .into())
    }
}

impl<'a> TryFrom<ValueWrap<'a>> for redis::Value {
    type Error = tank_core::Error;

    fn try_from(value: ValueWrap) -> Result<Self, Self::Error> {
        let value: tank_core::Value = value.into();
        Ok(match value {
            v if v.is_null() => redis::Value::Nil,
            tank_core::Value::Boolean(Some(v)) => redis::Value::Boolean(v),
            tank_core::Value::Int8(..)
            | tank_core::Value::Int16(..)
            | tank_core::Value::Int32(..)
            | tank_core::Value::Int64(..)
            | tank_core::Value::Int128(..)
            | tank_core::Value::UInt8(..)
            | tank_core::Value::UInt16(..)
            | tank_core::Value::UInt32(..)
            | tank_core::Value::UInt64(..)
            | tank_core::Value::UInt128(..) => {
                let tank_core::Value::Int64(Some(v)) =
                    value.try_as(&tank_core::Value::Int64(None))?
                else {
                    unreachable!("Unexpected error, it should be Int64 here");
                };
                redis::Value::Int(v)
            }
            tank_core::Value::Float32(..)
            | tank_core::Value::Float64(..)
            | tank_core::Value::Decimal(..) => {
                let tank_core::Value::Float64(Some(v)) =
                    value.try_as(&tank_core::Value::Float64(None))?
                else {
                    unreachable!("Unexpected error, it should be Float64 here");
                };
                redis::Value::Double(v)
            }
            tank_core::Value::Char(..)
            | tank_core::Value::Varchar(..)
            | tank_core::Value::Date(..)
            | tank_core::Value::Time(..)
            | tank_core::Value::Timestamp(..)
            | tank_core::Value::TimestampWithTimezone(..)
            | tank_core::Value::Interval(..)
            | tank_core::Value::Uuid(..)
            | tank_core::Value::Json(..)
            | tank_core::Value::Unknown(..) => {
                let tank_core::Value::Varchar(Some(v)) =
                    value.try_as(&tank_core::Value::Varchar(None))?
                else {
                    unreachable!("Unexpected error, it should be Varchar here");
                };
                redis::Value::BulkString(v.as_bytes().into())
            }
            tank_core::Value::Blob(Some(v)) => redis::Value::BulkString(v.into()),
            tank_core::Value::Array(Some(v), ..) => redis::Value::Array(
                v.into_iter()
                    .map(|v| {
                        let v: ValueWrap<'_> = v.into();
                        redis::Value::try_from(v)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            tank_core::Value::List(Some(v), ..) => redis::Value::Array(
                v.into_iter()
                    .map(|v| {
                        let v: ValueWrap<'_> = v.into();
                        redis::Value::try_from(v)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            tank_core::Value::Map(Some(v), ..) => redis::Value::Map(
                v.into_iter()
                    .map(|(k, v)| {
                        let k: ValueWrap<'_> = k.into();
                        let v: ValueWrap<'_> = v.into();
                        Ok::<_, tank_core::Error>((
                            redis::Value::try_from(k)?,
                            redis::Value::try_from(v)?,
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            tank_core::Value::Struct(Some(v), ..) => redis::Value::Map(
                v.into_iter()
                    .map(|(k, v)| {
                        let k: ValueWrap<'_> = tank_core::Value::Varchar(Some(k.into())).into();
                        let v: ValueWrap<'_> = v.into();
                        Ok::<_, tank_core::Error>((
                            redis::Value::try_from(k)?,
                            redis::Value::try_from(v)?,
                        ))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(tank_core::Error::msg("")),
        })
    }
}

impl<'a> FromRedisValue for ValueWrap<'a> {
    fn from_redis_value(value: redis::Value) -> Result<Self, redis::ParsingError> {
        Self::try_from(value).map_err(|e| format!("{e}").into())
    }
}

impl<'a> ToRedisArgs for ValueWrap<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let value = match &self.0 {
            Cow::Borrowed(v) => *v,
            Cow::Owned(v) => v,
        };
        match value {
            tank_core::Value::Boolean(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Int8(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Int16(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Int32(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Int64(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Int128(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::UInt8(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::UInt16(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::UInt32(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::UInt64(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::UInt128(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Float32(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Float64(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Decimal(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Char(Some(v), ..) => v.to_string().write_redis_args(out),
            tank_core::Value::Date(Some(..), ..)
            | tank_core::Value::Time(Some(..), ..)
            | tank_core::Value::Timestamp(Some(..), ..)
            | tank_core::Value::TimestampWithTimezone(Some(..), ..)
            | tank_core::Value::Interval(Some(..), ..)
            | tank_core::Value::Uuid(Some(..), ..) => {
                if let Ok(v) = value
                    .clone()
                    .try_as(&tank_core::Value::Varchar(None))
                    .inspect_err(|e| {
                        log::error!("{e:#}");
                    })
                {
                    ValueWrap(Cow::Owned(v)).write_redis_args(out);
                }
            }
            tank_core::Value::Varchar(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Blob(Some(v), ..) => v.write_redis_args(out),
            tank_core::Value::Array(Some(v), ..) => v
                .iter()
                .map(|v| ValueWrap(Cow::Borrowed(v)))
                .collect::<Vec<_>>()
                .write_redis_args(out),
            tank_core::Value::List(Some(v), ..) => v
                .iter()
                .map(|v| ValueWrap(Cow::Borrowed(v)))
                .collect::<Vec<_>>()
                .write_redis_args(out),
            tank_core::Value::Map(Some(v), ..) => v
                .iter()
                .map(|(k, v)| (ValueWrap(Cow::Borrowed(k)), ValueWrap(Cow::Borrowed(v))))
                .collect::<HashMap<_, _>>()
                .write_redis_args(out),
            tank_core::Value::Json(Some(v), ..) => v.to_string().write_redis_args(out),
            tank_core::Value::Struct(Some(v), ..) => v
                .iter()
                .map(|(k, v)| (k, ValueWrap(Cow::Borrowed(v))))
                .collect::<HashMap<_, _>>()
                .write_redis_args(out),
            tank_core::Value::Unknown(Some(v), ..) => v.write_redis_args(out),
            v if v.is_null() => None::<i32>.write_redis_args(out), // This writes nothing
            _ => {
                log::error!(
                    "tank::Value variant `{:?}` is not supported by Valkey/Redis",
                    &value
                )
            }
        }
    }
}
