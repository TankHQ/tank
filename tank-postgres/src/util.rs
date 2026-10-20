use crate::interval_wrap::IntervalWrap;
use anyhow::anyhow;
use fallible_iterator::FallibleIterator;
use postgres_protocol::types::{ArrayDimension, array_from_sql};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;
use std::{borrow::Cow, error::Error, mem};
use tank_core::{Value, ValueOwned};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;
use wtx::database::client::postgres::Ty;

/// Converts a `wtx` Postgres type into the canonical `postgres_types::Type`.
pub(crate) fn wtx_ty_to_pg_type(ty: Ty) -> Type {
    let oid: u32 = ty.into();
    Type::from_oid(oid).unwrap_or(Type::UNKNOWN)
}

/// A decode strategy for one result-set column, resolved once per statement.
///
/// `extract_value_resolved` matches the column's `postgres_types::Type` on every
/// cell. Since the column types of a statement are fixed, the match can be
/// resolved once and the per-cell path reduced to a discriminant switch on this
/// enum. The common scalar types decode straight through `postgres_protocol`,
/// skipping both the `Type` comparison and the `FromSql` dispatch; the remaining
/// types keep their resolved [`Type`] and go through the generic path.
#[derive(Debug, Clone)]
pub enum ColumnDecoder {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    UInt32,
    Text,
    Bytea,
    Unknown,
    /// Types decoded by `FromSql` with an already-resolved type.
    Typed(Type),
    /// Types decoded by the generic (array/range/enum/…) path.
    Generic(Type),
}

impl ColumnDecoder {
    /// Resolves the decoder for a column type once per statement.
    pub(crate) fn from_type(ty: &Type) -> Self {
        match ty.kind() {
            Kind::Simple => match *ty {
                Type::BOOL => Self::Bool,
                Type::CHAR => Self::Int8,
                Type::INT2 => Self::Int16,
                Type::INT4 => Self::Int32,
                Type::INT8 => Self::Int64,
                Type::FLOAT4 => Self::Float32,
                Type::FLOAT8 => Self::Float64,
                Type::OID => Self::UInt32,
                Type::VARCHAR | Type::TEXT | Type::NAME | Type::BPCHAR | Type::JSON | Type::XML => {
                    Self::Text
                }
                Type::BYTEA => Self::Bytea,
                Type::UNKNOWN => Self::Unknown,
                Type::NUMERIC
                | Type::DATE
                | Type::TIME
                | Type::TIMESTAMP
                | Type::TIMESTAMPTZ
                | Type::INTERVAL
                | Type::UUID => Self::Typed(ty.clone()),
                _ => Self::Generic(ty.clone()),
            },
            Kind::Pseudo | Kind::Enum(_) => Self::Unknown,
            _ => Self::Generic(ty.clone()),
        }
    }

    /// Decodes one cell with the pre-resolved strategy.
    #[inline]
    pub(crate) fn decode<'a>(
        &self,
        raw: Option<&'a [u8]>,
    ) -> Result<Value<'a>, Box<dyn Error + Sync + Send>> {
        use postgres_protocol::types;
        fn scalar<'a, T: FromSql<'a>>(
            ty: &Type,
            raw: Option<&'a [u8]>,
        ) -> Result<Option<T>, Box<dyn Error + Sync + Send>> {
            Ok(match raw {
                Some(raw) => Some(T::from_sql(ty, raw)?),
                None => None,
            })
        }
        Ok(match self {
            Self::Bool => Value::Boolean(match raw {
                Some(raw) => Some(types::bool_from_sql(raw)?),
                None => None,
            }),
            Self::Int8 => Value::Int8(match raw {
                Some(raw) => Some(types::char_from_sql(raw)?),
                None => None,
            }),
            Self::Int16 => Value::Int16(match raw {
                Some(raw) => Some(types::int2_from_sql(raw)?),
                None => None,
            }),
            Self::Int32 => Value::Int32(match raw {
                Some(raw) => Some(types::int4_from_sql(raw)?),
                None => None,
            }),
            Self::Int64 => Value::Int64(match raw {
                Some(raw) => Some(types::int8_from_sql(raw)?),
                None => None,
            }),
            Self::Float32 => Value::Float32(match raw {
                Some(raw) => Some(types::float4_from_sql(raw)?),
                None => None,
            }),
            Self::Float64 => Value::Float64(match raw {
                Some(raw) => Some(types::float8_from_sql(raw)?),
                None => None,
            }),
            Self::UInt32 => Value::UInt32(match raw {
                Some(raw) => Some(types::oid_from_sql(raw)?),
                None => None,
            }),
            Self::Text => match raw {
                Some(raw) => Value::Varchar(Some(Cow::Borrowed(types::text_from_sql(raw)?))),
                None => Value::Varchar(None),
            },
            Self::Bytea => match raw {
                Some(raw) => Value::Blob(Some(Cow::Borrowed(raw))),
                None => Value::Blob(None),
            },
            Self::Unknown => match raw {
                Some(raw) => Value::Unknown(Some(Cow::Borrowed(types::text_from_sql(raw)?))),
                None => Value::Unknown(None),
            },
            Self::Typed(ty) => match *ty {
                Type::NUMERIC => Value::Decimal(scalar::<Decimal>(ty, raw)?, 0, 0),
                Type::DATE => Value::Date(scalar::<Date>(ty, raw)?),
                Type::TIME => Value::Time(scalar::<Time>(ty, raw)?),
                Type::TIMESTAMP => Value::Timestamp(scalar::<PrimitiveDateTime>(ty, raw)?),
                Type::TIMESTAMPTZ => {
                    Value::TimestampWithTimezone(scalar::<OffsetDateTime>(ty, raw)?)
                }
                Type::INTERVAL => Value::Interval(scalar::<IntervalWrap>(ty, raw)?.map(Into::into)),
                Type::UUID => Value::Uuid(scalar::<Uuid>(ty, raw)?),
                _ => extract_value_resolved(ty, raw)?,
            },
            Self::Generic(ty) => extract_value_resolved(ty, raw)?,
        })
    }
}

/// Decodes a value whose canonical `postgres_types::Type` is already resolved.
///
/// Resolving the type once per column (rather than per cell) avoids a repeated
/// OID lookup on the hot path.
pub(crate) fn extract_value_resolved<'a>(
    ty: &Type,
    raw: Option<&'a [u8]>,
) -> Result<Value<'a>, Box<dyn Error + Sync + Send>> {
    extract_value_pg(ty, raw)
}

fn extract_value_pg<'a>(
    ty: &Type,
    raw: Option<&'a [u8]>,
) -> Result<Value<'a>, Box<dyn Error + Sync + Send>> {
    fn convert<'a, T: FromSql<'a>>(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Option<T>, Box<dyn Error + Sync + Send>> {
        Ok(match raw {
            Some(raw) => Some(T::from_sql(ty, raw)?),
            None => None,
        })
    }
    let kind = ty.kind();
    Ok(match kind {
        Kind::Simple => match *ty {
            Type::BOOL => Value::Boolean(convert::<bool>(ty, raw)?),
            Type::CHAR => Value::Int8(convert::<i8>(ty, raw)?),
            Type::INT2 => Value::Int16(convert::<i16>(ty, raw)?),
            Type::INT4 => Value::Int32(convert::<i32>(ty, raw)?),
            Type::INT8 => Value::Int64(convert::<i64>(ty, raw)?),
            Type::FLOAT4 => Value::Float32(convert::<f32>(ty, raw)?),
            Type::FLOAT8 => Value::Float64(convert::<f64>(ty, raw)?),
            Type::NUMERIC => Value::Decimal(convert::<Decimal>(ty, raw)?, 0, 0),
            Type::OID => Value::UInt32(convert::<u32>(ty, raw)?),
            Type::VARCHAR | Type::TEXT | Type::NAME | Type::BPCHAR | Type::JSON | Type::XML => {
                // Borrow the UTF-8 payload directly from the driver buffer.
                match raw {
                    Some(raw) => match std::str::from_utf8(raw) {
                        Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                        Err(_) => Value::Varchar(convert::<String>(ty, Some(raw))?.map(Into::into)),
                    },
                    None => Value::Varchar(None),
                }
            }
            Type::BYTEA => match raw {
                Some(raw) => Value::Blob(Some(Cow::Borrowed(raw))),
                None => Value::Blob(None),
            },
            Type::DATE => Value::Date(convert::<Date>(ty, raw)?),
            Type::TIME => Value::Time(convert::<Time>(ty, raw)?),
            Type::TIMESTAMP => Value::Timestamp(convert::<PrimitiveDateTime>(ty, raw)?),
            Type::TIMESTAMPTZ => Value::TimestampWithTimezone(convert::<OffsetDateTime>(ty, raw)?),
            Type::INTERVAL => Value::Interval(convert::<IntervalWrap>(ty, raw)?.map(Into::into)),
            Type::UUID => Value::Uuid(convert::<Uuid>(ty, raw)?),
            Type::UNKNOWN => match raw {
                Some(raw) => match std::str::from_utf8(raw) {
                    Ok(s) => Value::Unknown(Some(Cow::Borrowed(s))),
                    Err(_) => Value::Unknown(convert::<String>(ty, Some(raw))?.map(Into::into)),
                },
                None => Value::Unknown(None),
            },
            _ => {
                return Err(anyhow!("Unexpected {ty} variant for Kind::Simple").into());
            }
        },
        Kind::Array(inner_ty) => {
            // Arrays are not on the hot path; own their payloads so the returned
            // value does not borrow from locals (`dimensions`).
            let ty = extract_value_pg(inner_ty, None)?.into_owned();
            if let Some(raw) = raw {
                let array = array_from_sql(raw)?;
                let dimensions = array.dimensions().collect::<Vec<_>>()?;
                if dimensions.is_empty() {
                    Value::Array(Some(Vec::new().into()), Box::new(ty), 0)
                } else {
                    let mut values = array
                        .values()
                        .map(|v| extract_value_pg(inner_ty, v).map(Value::into_owned))
                        .collect::<Vec<_>>()?;
                    let mut cursor = 0;
                    build_array(&mut cursor, &mut values, &ty, dimensions.iter().cloned())?
                }
            } else {
                Value::List(None, Box::new(ty))
            }
        }
        // Pseudo types (e.g. `unknown`, `record`) and enums have no dedicated
        // binary decoder; surface the raw payload as an unknown value.
        Kind::Pseudo | Kind::Enum(_) => match raw {
            Some(raw) => {
                Value::Unknown(Some(Cow::Borrowed(std::str::from_utf8(raw).unwrap_or(""))))
            }
            None => Value::Unknown(None),
        },
        _ => return Err(tank_core::anyhow!("Unexpected kind {kind:?}").into()),
    })
}

fn build_array(
    cursor: &mut usize,
    values: &mut Vec<ValueOwned>,
    element_ty: &ValueOwned,
    mut it: impl ExactSizeIterator<Item = ArrayDimension> + Clone,
) -> Result<ValueOwned, Box<dyn Error + Sync + Send>> {
    let dimension = it.next().expect("Must have one dimension at least");
    let len = dimension.len as u32;
    Ok(if it.len() == 0 {
        let begin = *cursor;
        let end = begin + len as usize;
        *cursor = end;
        Value::Array(
            Some((begin..end).map(|i| mem::take(&mut values[i])).collect()),
            Box::new(element_ty.clone()),
            len,
        )
    } else {
        let mut elements = Vec::with_capacity(len as usize);
        let mut nested_ty = element_ty.clone();
        for _ in 0..len {
            let child = build_array(cursor, values, element_ty, it.clone())?;
            nested_ty = child.as_null();
            elements.push(child);
        }
        Value::Array(Some(elements.into_boxed_slice()), Box::new(nested_ty), len)
    })
}

pub(crate) fn flatten_array<'a>(
    values: &'a [Value<'a>],
    mut element_type: &'a Value<'a>,
) -> (Vec<&'a Value<'a>>, Vec<ArrayDimension>, Type) {
    let mut dimensions = Vec::new();
    let mut current_level = values;
    let mut total_capacity = 1;
    loop {
        let len = current_level.len();
        dimensions.push(ArrayDimension {
            len: len as i32,
            lower_bound: 1,
        });
        total_capacity *= len;
        if len == 0 {
            total_capacity = 0;
            break;
        }
        match current_level.first() {
            Some(Value::Array(Some(inner), ..)) => current_level = inner,
            Some(Value::List(Some(inner), ..)) => current_level = inner.as_slice(),
            _ => break,
        }
    }
    loop {
        match element_type {
            Value::Array(.., element, _) | Value::List(.., element) => element_type = element,
            _ => break,
        }
    }
    let pg_type = value_to_postgres_type(element_type);
    let mut collector = Vec::with_capacity(total_capacity);
    flatten_recursive(values, &mut collector);
    (collector, dimensions, pg_type)
}

fn flatten_recursive<'a>(values: &'a [Value<'a>], collector: &mut Vec<&'a Value<'a>>) {
    for value in values {
        match value {
            Value::Array(Some(inner), ..) => {
                flatten_recursive(inner, collector);
            }
            Value::List(Some(inner), ..) => {
                flatten_recursive(inner, collector);
            }
            Value::Array(None, ..) | Value::List(None, ..) => {}
            _ => collector.push(value),
        }
    }
}

pub fn value_to_postgres_type(value: &Value) -> Type {
    assert!(matches!(Type::BPCHAR_ARRAY.kind(), Kind::Array(..)));
    match value {
        Value::Boolean(..) => Type::BOOL,
        Value::Int8(..) => Type::INT2,
        Value::Int16(..) => Type::INT2,
        Value::Int32(..) => Type::INT4,
        Value::Int64(..) => Type::INT8,
        Value::Int128(..) => Type::NUMERIC,
        Value::UInt8(..) => Type::INT2,
        Value::UInt16(..) => Type::INT4,
        Value::UInt32(..) => Type::INT8,
        Value::UInt64(..) => Type::NUMERIC,
        Value::UInt128(..) => Type::NUMERIC,
        Value::Float32(..) => Type::FLOAT4,
        Value::Float64(..) => Type::FLOAT8,
        Value::Decimal(.., _, _) => Type::NUMERIC,
        Value::Char(..) => Type::BPCHAR,
        Value::Varchar(..) => Type::TEXT,
        Value::Blob(..) => Type::BYTEA,
        Value::Date(..) => Type::DATE,
        Value::Time(..) => Type::TIME,
        Value::Timestamp(..) => Type::TIMESTAMP,
        Value::TimestampWithTimezone(..) => Type::TIMESTAMPTZ,
        Value::Interval(..) => Type::INTERVAL,
        Value::Uuid(..) => Type::UUID,
        Value::Array(.., element, _) | Value::List(.., element) => match **element {
            Value::Boolean(..) => Type::BOOL_ARRAY,
            Value::Int8(..) => Type::INT2_ARRAY,
            Value::Int16(..) => Type::INT2_ARRAY,
            Value::Int32(..) => Type::INT4_ARRAY,
            Value::Int64(..) => Type::INT8_ARRAY,
            Value::Int128(..) => Type::NUMERIC_ARRAY,
            Value::UInt8(..) => Type::INT2_ARRAY,
            Value::UInt16(..) => Type::INT4_ARRAY,
            Value::UInt32(..) => Type::INT8_ARRAY,
            Value::UInt64(..) => Type::NUMERIC_ARRAY,
            Value::UInt128(..) => Type::NUMERIC_ARRAY,
            Value::Float32(..) => Type::FLOAT4_ARRAY,
            Value::Float64(..) => Type::FLOAT8_ARRAY,
            Value::Decimal(..) => Type::NUMERIC_ARRAY,
            Value::Char(..) => Type::BPCHAR_ARRAY,
            Value::Varchar(..) => Type::TEXT_ARRAY,
            Value::Blob(..) => Type::BYTEA_ARRAY,
            Value::Date(..) => Type::DATE_ARRAY,
            Value::Time(..) => Type::TIME_ARRAY,
            Value::Timestamp(..) => Type::TIMESTAMP_ARRAY,
            Value::TimestampWithTimezone(..) => Type::TIMESTAMPTZ_ARRAY,
            Value::Interval(..) => Type::INTERVAL_ARRAY,
            Value::Uuid(..) => Type::UUID_ARRAY,
            Value::Array(..) | Value::List(..) => value_to_postgres_type(element),
            Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON_ARRAY,
            _ => Type::UNKNOWN,
        },
        Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON,
        _ => Type::UNKNOWN,
    }
}
