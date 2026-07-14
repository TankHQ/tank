use anyhow::anyhow;
use klickhouse::{Type, Value as KlValue};
use rust_decimal::Decimal;
use std::fmt::Write as _;
use std::{borrow::Cow, collections::HashMap};
use tank_core::{Result, Value};
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};
use uuid::Uuid;

fn format_datetime(odt: OffsetDateTime) -> String {
    let odt = odt.to_offset(UtcOffset::UTC);
    let mut out = String::new();
    let _ = write!(
        out,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        odt.year(),
        odt.month() as u8,
        odt.day(),
        odt.hour(),
        odt.minute(),
        odt.second(),
    );
    let nanos = odt.nanosecond();
    if nanos != 0 {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push('.');
        out.push_str(&frac);
    }
    out
}

/// Convert a klickhouse value to a tank value.
pub(crate) fn kl_to_tank(ty: &Type, val: KlValue) -> Result<Value> {
    match ty {
        Type::Nullable(inner) => {
            return match val {
                KlValue::Null => Ok(Value::Null),
                other => kl_to_tank(inner, other),
            };
        }
        Type::LowCardinality(inner) => return kl_to_tank(inner, val),
        _ => {}
    }

    match val {
        KlValue::Null => Ok(Value::Null),

        KlValue::Int8(v) => Ok(Value::Int8(Some(v))),
        KlValue::Int16(v) => Ok(Value::Int16(Some(v))),
        KlValue::Int32(v) => Ok(Value::Int32(Some(v))),
        KlValue::Int64(v) => Ok(Value::Int64(Some(v))),
        KlValue::Int128(v) => Ok(Value::Int128(Some(v))),
        KlValue::UInt8(v) => Ok(Value::UInt8(Some(v))),
        KlValue::UInt16(v) => Ok(Value::UInt16(Some(v))),
        KlValue::UInt32(v) => Ok(Value::UInt32(Some(v))),
        KlValue::UInt64(v) => Ok(Value::UInt64(Some(v))),
        KlValue::UInt128(v) => Ok(Value::UInt128(Some(v))),

        KlValue::Float32(v) => Ok(Value::Float32(Some(v))),
        KlValue::Float64(v) => Ok(Value::Float64(Some(v))),

        KlValue::Decimal32(scale, raw) => {
            let (p, s) = decimal_ps(ty, scale);
            Ok(Value::Decimal(Some(Decimal::from_i128_with_scale(raw as i128, s as u32)), p, s))
        }
        KlValue::Decimal64(scale, raw) => {
            let (p, s) = decimal_ps(ty, scale);
            Ok(Value::Decimal(Some(Decimal::from_i128_with_scale(raw as i128, s as u32)), p, s))
        }
        KlValue::Decimal128(scale, raw) => {
            let (p, s) = decimal_ps(ty, scale);
            Ok(Value::Decimal(Some(Decimal::from_i128_with_scale(raw, s as u32)), p, s))
        }

        KlValue::String(bytes) => {
            let s = String::from_utf8_lossy(&bytes).into_owned();
            Ok(Value::Varchar(Some(Cow::Owned(s))))
        }

        KlValue::Date(d) => {
            let secs = d.0 as i64 * 86_400;
            let date = OffsetDateTime::from_unix_timestamp(secs)
                .map_err(|e| anyhow!("Invalid Date from klickhouse: {e}"))?
                .date();
            Ok(Value::Date(Some(date)))
        }

        KlValue::DateTime(dt) => {
            let odt = OffsetDateTime::from_unix_timestamp(dt.1 as i64)
                .map_err(|e| anyhow!("Invalid DateTime from klickhouse: {e}"))?;
            match ty {
                Type::DateTime(tz) if tz.name() != "UTC" => {
                    Ok(Value::TimestampWithTimezone(Some(odt)))
                }
                _ => Ok(Value::Timestamp(Some(PrimitiveDateTime::new(odt.date(), odt.time())))),
            }
        }

        KlValue::DateTime64(dt64) => {
            let precision = dt64.2 as u32;
            if precision > 9 {
                let ticks = dt64.1 as i64 as i128;
                let scale_down = 10i128
                    .checked_pow(precision - 9)
                    .ok_or_else(|| anyhow!("Unsupported DateTime64 precision: {precision}"))?;
                let nanos = ticks.div_euclid(scale_down);
                let odt = OffsetDateTime::from_unix_timestamp_nanos(nanos)
                    .map_err(|e| anyhow!("Invalid DateTime64 from klickhouse: {e}"))?;
                return Ok(Value::Varchar(Some(Cow::Owned(format_datetime(odt)))));
            }
            let ticks = dt64.1 as i64;
            let factor = 10i64.pow(precision);
            let secs = ticks.div_euclid(factor);
            let sub = ticks.rem_euclid(factor) as u64;
            let nanos = sub * 10u64.pow(9 - precision);
            let odt = OffsetDateTime::from_unix_timestamp_nanos(
                secs as i128 * 1_000_000_000 + nanos as i128,
            )
            .map_err(|e| anyhow!("Invalid DateTime64 from klickhouse: {e}"))?;
            match ty {
                Type::DateTime64(_, tz) if tz.name() != "UTC" => {
                    Ok(Value::TimestampWithTimezone(Some(odt)))
                }
                _ => Ok(Value::Timestamp(Some(PrimitiveDateTime::new(odt.date(), odt.time())))),
            }
        }

        KlValue::Uuid(u) => Ok(Value::Uuid(Some(Uuid::from_bytes(*u.as_bytes())))),

        KlValue::Array(elements) => {
            let inner_ty = match ty {
                Type::Array(inner) => inner.as_ref(),
                _ => return Err(anyhow!("Expected Array type, got {ty:?}")),
            };
            let inner_proto = kl_type_proto(inner_ty);
            let values: Result<Vec<Value>> =
                elements.into_iter().map(|e| kl_to_tank(inner_ty, e)).collect();
            Ok(Value::List(Some(values?), Box::new(inner_proto)))
        }

        KlValue::Map(keys, vals) => {
            let (key_ty, val_ty) = match ty {
                Type::Map(k, v) => (k.as_ref(), v.as_ref()),
                _ => return Err(anyhow!("Expected Map type, got {ty:?}")),
            };
            let key_proto = kl_type_proto(key_ty);
            let val_proto = kl_type_proto(val_ty);
            let mut map = HashMap::new();
            for (k, v) in keys.into_iter().zip(vals) {
                map.insert(kl_to_tank(key_ty, k)?, kl_to_tank(val_ty, v)?);
            }
            Ok(Value::Map(
                Some(map),
                Box::new(key_proto),
                Box::new(val_proto),
            ))
        }

        KlValue::Enum8(v) => Ok(Value::Int8(Some(v))),
        KlValue::Enum16(v) => Ok(Value::Int16(Some(v))),

        other => {
            let s = format!("{other:?}");
            Ok(Value::Unknown(Some(s)))
        }
    }
}

/// Build a null prototype for nested collection types.
pub(crate) fn kl_type_proto(ty: &Type) -> Value {
    match ty {
        Type::Int8 => Value::Int8(None),
        Type::Int16 => Value::Int16(None),
        Type::Int32 => Value::Int32(None),
        Type::Int64 => Value::Int64(None),
        Type::Int128 => Value::Int128(None),
        Type::UInt8 => Value::UInt8(None),
        Type::UInt16 => Value::UInt16(None),
        Type::UInt32 => Value::UInt32(None),
        Type::UInt64 => Value::UInt64(None),
        Type::UInt128 => Value::UInt128(None),
        Type::Float32 => Value::Float32(None),
        Type::Float64 => Value::Float64(None),
        Type::Decimal32(s) | Type::Decimal64(s) | Type::Decimal128(s) => {
            Value::Decimal(None, 0, *s as u8)
        }
        Type::String | Type::FixedString(_) => Value::Varchar(None),
        Type::Uuid => Value::Uuid(None),
        Type::Date => Value::Date(None),
        Type::DateTime(tz) => {
            if tz.name() == "UTC" {
                Value::Timestamp(None)
            } else {
                Value::TimestampWithTimezone(None)
            }
        }
        Type::DateTime64(_, tz) => {
            if tz.name() == "UTC" {
                Value::Timestamp(None)
            } else {
                Value::TimestampWithTimezone(None)
            }
        }
        Type::Nullable(inner) | Type::LowCardinality(inner) => kl_type_proto(inner),
        Type::Array(inner) => Value::List(None, Box::new(kl_type_proto(inner))),
        Type::Map(k, v) => {
            Value::Map(None, Box::new(kl_type_proto(k)), Box::new(kl_type_proto(v)))
        }
        _ => Value::Unknown(None),
    }
}

fn decimal_ps(ty: &Type, scale: usize) -> (u8, u8) {
    let s = scale as u8;
    match ty {
        Type::Decimal32(_) => (9, s),
        Type::Decimal64(_) => (18, s),
        Type::Decimal128(_) => (38, s),
        _ => (18, s),
    }
}
