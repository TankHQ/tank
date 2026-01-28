use mongodb::bson::{self, Binary, Bson, Document, spec::BinarySubtype};
use std::{borrow::Cow, cell::OnceCell, collections::HashMap};
use tank_core::{AsValue, Error, Result, Value, print_timer};
use time::PrimitiveDateTime;

pub fn value_to_bson(v: &Value) -> Result<Bson> {
    Ok(match v {
        _ if v.is_null() => Bson::Null,
        Value::Boolean(Some(v), ..) => Bson::Boolean(*v),
        Value::Int8(Some(v), ..) => Bson::Int32(*v as i32),
        Value::Int16(Some(v), ..) => Bson::Int32(*v as i32),
        Value::Int32(Some(v), ..) => Bson::Int32(*v),
        Value::Int64(Some(v), ..) => Bson::Int64(*v),
        Value::UInt8(Some(v), ..) => Bson::Int32(*v as i32),
        Value::UInt16(Some(v), ..) => Bson::Int32(*v as i32),
        Value::UInt32(Some(v), ..) => Bson::Int64(*v as i64),
        Value::UInt64(Some(..), ..) => Bson::Int64(i64::try_from_value(v.clone())?),
        Value::Float32(Some(v), ..) => Bson::Double(*v as f64),
        Value::Float64(Some(v), ..) => Bson::Double(*v),
        Value::Decimal(Some(..), ..) => Bson::Double(f64::try_from_value(v.clone())?),
        Value::Char(Some(v), ..) => Bson::String(v.to_string()),
        Value::Varchar(Some(v), ..) => Bson::String(v.to_string()),
        Value::Blob(Some(v), ..) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.clone().into_vec(),
        }),
        Value::Date(Some(v), ..) => {
            let midnight = time::Time::MIDNIGHT;
            let date_time = PrimitiveDateTime::new(*v, midnight).assume_utc();
            Bson::DateTime(bson::DateTime::from_millis(
                (date_time.unix_timestamp_nanos() / 1_000_000) as _,
            ))
        }
        Value::Time(Some(v), ..) => {
            let mut out = String::new();
            print_timer(
                &mut out,
                "",
                v.hour() as _,
                v.minute(),
                v.second(),
                v.nanosecond(),
            );
            Bson::String(out)
        }
        Value::Timestamp(Some(v), ..) => {
            let ms = v.assume_utc().unix_timestamp_nanos() / 1_000_000;
            Bson::DateTime(bson::DateTime::from_millis(ms as _))
        }
        Value::TimestampWithTimezone(Some(v), ..) => {
            let ms = v.to_utc().unix_timestamp_nanos() / 1_000_000;
            Bson::DateTime(bson::DateTime::from_millis(ms as _))
        }
        Value::Uuid(Some(v), ..) => Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: v.as_bytes().to_vec(),
        }),
        Value::Array(Some(v), ..) => {
            Bson::Array(v.iter().map(value_to_bson).collect::<Result<_>>()?)
        }
        Value::List(Some(v), ..) => {
            Bson::Array(v.iter().map(value_to_bson).collect::<Result<_>>()?)
        }
        Value::Map(Some(v), ..) => {
            let mut doc = Document::new();
            for (k, v) in v.iter() {
                let Ok(k) = String::try_from_value(k.clone()) else {
                    return Err(Error::msg(format!(
                        "Unexpected tank::Value key: {k:?}, it is not convertible to String"
                    )));
                };
                let v = value_to_bson(v)?;
                doc.insert(k, v);
            }
            Bson::Document(doc)
        }
        Value::Json(Some(v), ..) => bson::to_bson(v)?,
        Value::Struct(Some(v), ..) => {
            let mut doc = Document::new();
            for (k, v) in v.iter() {
                let v = value_to_bson(v)?;
                doc.insert(k, v);
            }
            Bson::Document(doc)
        }
        Value::Unknown(Some(v), ..) => Bson::String(v.clone()),
        _ => {
            return Err(Error::msg(format!(
                "Unexpected tank::Value, MongoDB does not support {v:?}"
            )));
        }
    })
}

pub fn bson_to_value(bson: &Bson) -> Result<Value> {
    Ok(match bson {
        Bson::Null => Value::Null,
        Bson::Boolean(v) => Value::Boolean(Some(*v)),
        Bson::Int32(v) => Value::Int32(Some(*v)),
        Bson::Int64(v) => Value::Int64(Some(*v)),
        Bson::Double(v) => Value::Float64(Some(*v)),
        Bson::Decimal128(v) => Value::Decimal(Some(v.to_string().parse()?), 0, 0),
        Bson::String(v) => Value::Varchar(Some(Cow::Owned(v.clone()))),
        Bson::Binary(bin) => match bin.subtype {
            BinarySubtype::Uuid => {
                let uuid = uuid::Uuid::from_slice(&bin.bytes)?;
                Value::Uuid(Some(uuid))
            }
            _ => Value::Blob(Some(bin.bytes.clone().into())),
        },
        Bson::DateTime(date_time) => {
            let ms = date_time.timestamp_millis();
            let nanos = (ms as i128) * 1_000_000;
            let date_time = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)?;
            Value::Timestamp(Some(PrimitiveDateTime::new(
                date_time.date(),
                date_time.time(),
            )))
        }
        Bson::Array(arr) => {
            let values = arr.iter().map(bson_to_value).collect::<Result<Box<_>>>()?;
            let len = values.len();
            let array_type = Box::new(if let Some(first) = values.first() {
                first.as_null()
            } else {
                Value::Unknown(None)
            });
            Value::Array(Some(values), array_type, len as _)
        }
        Bson::Document(doc) => {
            let mut map = HashMap::new();
            let mut k_type = OnceCell::new();
            let mut v_type = OnceCell::new();
            for (k, v) in doc.iter() {
                let k = k.clone().as_value();
                let v = bson_to_value(v)?;
                if k_type.get().is_none() {
                    k_type.set(k.as_null());
                    v_type.set(v.as_null());
                }
                map.insert(k, v);
            }
            Value::Map(
                Some(map),
                Box::new(k_type.take().unwrap_or_else(|| Value::Unknown(None))),
                Box::new(v_type.take().unwrap_or_else(|| Value::Unknown(None))),
            )
        }
        Bson::ObjectId(id) => {
            let mut padded = [0u8; 16];
            let bytes = id.bytes();
            padded[16 - bytes.len()..].copy_from_slice(&bytes);
            u128::from_be_bytes(padded).as_value()
        }
        _ => {
            return Err(Error::msg(format!("Unexpected Bson type: {bson:?}")));
        }
    })
}
