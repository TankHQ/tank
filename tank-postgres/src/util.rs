use crate::{ValueWrap, interval_wrap::IntervalWrap};
use anyhow::anyhow;
use postgres_protocol::types::{ArrayDimension, array_from_sql};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;
use std::{borrow::Cow, error::Error, mem};
use tank_core::{RowOwned, Value, ValueOwned};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use tokio_postgres::fallible_iterator::FallibleIterator;
use uuid::Uuid;

/// True if `sql` contains more than one statement.
///
/// Semicolons inside single-quoted string literals are ignored. The extended
/// query protocol used by `query_raw` rejects multi-command strings, so callers
/// route such queries through the simple protocol instead.
pub(crate) fn has_multiple_statements(sql: &str) -> bool {
    let mut statements = 0;
    let mut in_string = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if in_string => {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                } else {
                    in_string = false;
                }
            }
            '\'' => in_string = true,
            ';' if !in_string => {
                if chars.clone().any(|c| !c.is_whitespace()) {
                    statements += 1;
                }
            }
            _ => {}
        }
    }
    statements > 0
}

/// Decodes a simple-query row (raw SQL, no type info) into an owning tank row.
pub(crate) fn simple_query_row_to_tank_row_owned(
    labels: &tank_core::RowLabels,
    row: tokio_postgres::SimpleQueryRow,
) -> tank_core::Result<RowOwned> {
    let mut values = Vec::with_capacity(row.len());
    for i in 0..row.len() {
        let value = match row.try_get(i) {
            Ok(Some(v)) => ValueWrap::from_sql(&Type::UNKNOWN, v.as_bytes())
                .map(|v| v.0.into_owned().into_owned())
                .map_err(|e| anyhow!("{e:#}"))?,
            Ok(None) => Value::Null,
            Err(..) => {
                let col = &row.columns()[i];
                return Err(anyhow!(
                    "Could not deserialize column {} `{}`",
                    i,
                    col.name(),
                ));
            }
        };
        values.push(value);
    }
    Ok(tank_core::Row {
        labels: labels.clone(),
        values: values.into(),
    })
}

pub(crate) fn extract_value<'a>(
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
            let ty = extract_value(inner_ty, None)?.into_owned();
            if let Some(raw) = raw {
                let array = array_from_sql(raw)?;
                let dimensions = array.dimensions().collect::<Vec<_>>()?;
                if dimensions.is_empty() {
                    Value::Array(Some(Vec::new().into()), Box::new(ty), 0)
                } else {
                    let mut values = array
                        .values()
                        .map(|v| extract_value(inner_ty, v).map(Value::into_owned))
                        .collect::<Vec<_>>()?;
                    let mut cursor = 0;
                    build_array(&mut cursor, &mut values, &ty, dimensions.iter().cloned())?
                }
            } else {
                Value::List(None, Box::new(ty))
            }
        }
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

pub fn postgres_type_to_value(ty: &Type) -> Value<'_> {
    match *ty {
        Type::BOOL => Value::Boolean(None),
        Type::CHAR => Value::Int8(None),
        Type::INT2 => Value::Int16(None),
        Type::INT4 => Value::Int32(None),
        Type::INT8 => Value::Int64(None),
        Type::FLOAT4 => Value::Float32(None),
        Type::FLOAT8 => Value::Float64(None),
        Type::NUMERIC => Value::Decimal(None, 0, 0),
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::XML => Value::Varchar(None),
        Type::BYTEA => Value::Blob(None),
        Type::DATE => Value::Date(None),
        Type::TIME => Value::Time(None),
        Type::TIMESTAMP => Value::Timestamp(None),
        Type::TIMESTAMPTZ => Value::TimestampWithTimezone(None),
        Type::INTERVAL => Value::Interval(None),
        Type::UUID => Value::Uuid(None),
        Type::BOOL_ARRAY => Value::List(None, Box::new(Value::Boolean(None))),
        Type::INT2_ARRAY => Value::List(None, Box::new(Value::Int16(None))),
        Type::INT4_ARRAY => Value::List(None, Box::new(Value::Int32(None))),
        Type::INT8_ARRAY => Value::List(None, Box::new(Value::Int64(None))),
        Type::FLOAT4_ARRAY => Value::List(None, Box::new(Value::Float32(None))),
        Type::FLOAT8_ARRAY => Value::List(None, Box::new(Value::Float64(None))),
        Type::NUMERIC_ARRAY => Value::List(None, Box::new(Value::Decimal(None, 0, 0))),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => Value::List(None, Box::new(Value::Varchar(None))),
        Type::BYTEA_ARRAY => Value::List(None, Box::new(Value::Blob(None))),
        Type::DATE_ARRAY => Value::List(None, Box::new(Value::Date(None))),
        Type::TIME_ARRAY => Value::List(None, Box::new(Value::Time(None))),
        Type::TIMESTAMP_ARRAY => Value::List(None, Box::new(Value::Timestamp(None))),
        Type::TIMESTAMPTZ_ARRAY => Value::List(None, Box::new(Value::TimestampWithTimezone(None))),
        Type::INTERVAL_ARRAY => Value::List(None, Box::new(Value::Interval(None))),
        Type::UUID_ARRAY => Value::List(None, Box::new(Value::Uuid(None))),
        Type::JSON_ARRAY => Value::List(None, Box::new(Value::Json(None))),
        Type::JSON => Value::Json(None),
        _ => Value::Null,
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
