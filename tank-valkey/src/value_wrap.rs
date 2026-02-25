use redis::FromRedisValue;
use std::collections::HashMap;

#[derive(Default, Debug)]
pub(crate) struct ValueWrap(pub(crate) tank_core::Value);

impl FromRedisValue for ValueWrap {
    fn from_redis_value(value: redis::Value) -> Result<Self, redis::ParsingError> {
        Ok(ValueWrap(match value {
            redis::Value::Nil => tank_core::Value::Null,
            redis::Value::Int(v) => tank_core::Value::Int64(Some(v)),
            redis::Value::BulkString(items) => tank_core::Value::Varchar(Some(
                String::from_utf8(items).map_err(|e| e.to_string())?.into(),
            )),
            redis::Value::Array(v) | redis::Value::Set(v) => {
                let mut it = v
                    .into_iter()
                    .map(|v| Ok::<_, redis::ParsingError>(ValueWrap::from_redis_value(v)?.0))
                    .peekable();
                let ty = it
                    .peek()
                    .unwrap_or(&Ok(tank_core::Value::Null))
                    .as_ref()
                    .map_err(Clone::clone)?
                    .as_null();
                tank_core::Value::List(Some(it.collect::<Result<Vec<_>, _>>()?), Box::new(ty))
            }
            redis::Value::SimpleString(v) => tank_core::Value::Varchar(Some(v.into())),
            redis::Value::Map(v) => {
                let mut it = v
                    .into_iter()
                    .map(|(k, v)| {
                        Ok::<_, redis::ParsingError>((
                            ValueWrap::from_redis_value(k)?.0,
                            ValueWrap::from_redis_value(v)?.0,
                        ))
                    })
                    .peekable();
                let (k_ty, v_ty) = it
                    .peek()
                    .unwrap_or(&Ok((tank_core::Value::Null, tank_core::Value::Null)))
                    .as_ref()
                    .map(|(k, v)| (k.as_null(), v.as_null()))
                    .map_err(Clone::clone)?;
                tank_core::Value::Map(
                    Some(it.collect::<Result<HashMap<_, _>, _>>()?),
                    Box::new(k_ty.as_null()),
                    Box::new(v_ty.as_null()),
                )
            }
            redis::Value::Attribute { data, .. } => ValueWrap::from_redis_value(*data)?.0,
            redis::Value::Double(v) => tank_core::Value::Float64(Some(v)),
            redis::Value::Boolean(v) => tank_core::Value::Boolean(Some(v)),
            redis::Value::VerbatimString { text, .. } => {
                tank_core::Value::Varchar(Some(text.into()))
            }
            redis::Value::BigNumber(v) => tank_core::Value::Varchar(Some(v.to_string().into())),
            v => {
                return Err(format!("Unexpected {v:?} Valkey value").into());
            }
        }))
    }
}
