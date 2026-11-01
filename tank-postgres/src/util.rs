use crate::{ValueWrap, interval_wrap::IntervalWrap};
use anyhow::anyhow;
use async_stream::try_stream;
use postgres_protocol::types::{ArrayDimension, array_from_sql};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;
use std::{borrow::Cow, error::Error, iter, mem, pin::pin};
use tank_core::{
    CollectionType, ErrorContext, RowOwned, Value, ValueOwned,
    stream::{Stream, StreamExt},
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use tokio_postgres::{SimpleQueryMessage, fallible_iterator::FallibleIterator};
use uuid::Uuid;

pub(crate) fn row_to_tank_row(row: tokio_postgres::Row) -> tank_core::Result<Vec<ValueOwned>> {
    (0..row.len())
        .map(|i| match row.try_get::<_, ValueWrap>(i) {
            Ok(v) => Ok(v.0.into_owned().into_owned()),
            Err(e) => {
                let col = &row.columns()[i];
                Err(e).context(format!(
                    "Unknown type {} for column {} (index: {})",
                    col.type_(),
                    col.name(),
                    i,
                ))
            }
        })
        .collect()
}

pub(crate) fn simple_query_row_to_tank_row(
    row: tokio_postgres::SimpleQueryRow,
) -> tank_core::Result<Vec<ValueOwned>> {
    (0..row.len())
        .map(|i| match row.try_get(i) {
            Ok(Some(v)) => ValueWrap::from_sql(&Type::UNKNOWN, v.as_bytes())
                .map(|v| v.0.into_owned().into_owned())
                .map_err(|e| anyhow!("{e:#}")),
            Ok(None) => Ok(Value::Null),
            Err(..) => {
                let col = &row.columns()[i];
                Err(anyhow!(
                    "Could not deserialize column {} `{}`",
                    i,
                    col.name(),
                ))
            }
        })
        .collect()
}

pub(crate) fn stream_postgres_row_to_tank_row<V>(
    stream: impl AsyncFnOnce() -> tank_core::Result<V>,
) -> impl Stream<Item = tank_core::Result<tank_core::QueryResult>>
where
    V: Stream<Item = Result<tokio_postgres::Row, tokio_postgres::Error>>,
{
    try_stream! {
        let stream = stream().await?;
        let mut stream = pin!(stream);
        let mut labels: Option<tank_core::RowLabels> = None;
        while let Some(row) = stream.next().await.transpose()? {
            let labels = labels.get_or_insert_with(|| {
                row.columns().iter().map(|c| c.name().to_string()).collect()
            });
            yield RowOwned::new(labels.clone(), row_to_tank_row(row)?).into();
        }
    }
}

pub(crate) fn stream_postgres_simple_query_message_to_tank_query_result<V>(
    stream: impl AsyncFnOnce() -> tank_core::Result<V>,
) -> impl Stream<Item = tank_core::Result<tank_core::QueryResult>>
where
    V: Stream<Item = Result<tokio_postgres::SimpleQueryMessage, tokio_postgres::Error>>,
{
    try_stream! {
        let stream = stream().await?;
        let mut stream = pin!(stream);
        let mut labels: Option<tank_core::RowLabels> = None;
        let mut rows = false;
        while let Some(entry) = stream.next().await.transpose()? {
            match entry {
                SimpleQueryMessage::Row(row) => {
                    yield tank_core::QueryResult::Row(RowOwned::new(
                        labels
                            .as_ref()
                            .filter(|v| v.len() == row.len())
                            .ok_or_else(|| {
                                anyhow!(
                                    "Row columns names does not match the row currently received",
                                )
                            })?
                            .clone(),
                        simple_query_row_to_tank_row(row)?,
                    ));
                }
                SimpleQueryMessage::CommandComplete(rows_affected) => {
                    if rows {
                        // After a set of rows, a CommandComplete is received, ignore it
                        rows = false;
                    } else {
                        yield tank_core::QueryResult::Affected(tank_core::RowsAffected {
                            rows_affected: Some(rows_affected),
                            ..Default::default()
                        });
                    }
                }
                SimpleQueryMessage::RowDescription(columns) => {
                    rows = true;
                    labels = Some(
                        columns
                            .into_iter()
                            .map(|col| col.name().into())
                            .collect::<tank_core::RowLabels>(),
                    );
                    if columns.is_empty() {
                        log::warn!("The row description contains no columns, this can be expected but it can also be symthon of a wrong query")
                    }
                }
                _ => {}
            }
        }
    }
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
                Value::Varchar(convert::<&str>(ty, raw)?.map(Cow::Borrowed))
            }
            Type::BYTEA => Value::Blob(convert::<&[u8]>(ty, raw)?.map(Cow::Borrowed)),
            Type::DATE => Value::Date(convert::<Date>(ty, raw)?),
            Type::TIME => Value::Time(convert::<Time>(ty, raw)?),
            Type::TIMESTAMP => Value::Timestamp(convert::<PrimitiveDateTime>(ty, raw)?),
            Type::TIMESTAMPTZ => Value::TimestampWithTimezone(convert::<OffsetDateTime>(ty, raw)?),
            Type::INTERVAL => Value::Interval(convert::<IntervalWrap>(ty, raw)?.map(Into::into)),
            Type::UUID => Value::Uuid(convert::<Uuid>(ty, raw)?),
            Type::UNKNOWN => Value::Unknown(convert::<&str>(ty, raw)?.map(Cow::Borrowed)),
            _ => {
                return Err(anyhow!("Unexpected {ty} variant for Kind::Simple").into());
            }
        },
        Kind::Array(inner_ty) => {
            let ty = extract_value(inner_ty, None)?;
            if let Some(raw) = raw {
                let array = array_from_sql(raw)?;
                let mut values = array
                    .values()
                    .map(|v| extract_value(inner_ty, v))
                    .collect::<Vec<_>>()?;
                let dimensions = array.dimensions().collect::<Vec<_>>()?;
                if dimensions.is_empty() {
                    Value::Array(
                        Some(Vec::new().into()),
                        Box::new(CollectionType {
                            element: ty,
                            len: 0,
                        }),
                    )
                } else {
                    build_array(0, &mut values, &ty, dimensions.iter())?
                }
            } else {
                Value::List(
                    None,
                    Box::new(CollectionType {
                        element: ty,
                        len: 0,
                    }),
                )
            }
        }
        _ => return Err(tank_core::anyhow!("Unexpected kind {kind:?}").into()),
    })
}

fn build_array<'a, 'b>(
    begin: usize,
    values: &mut Vec<Value<'a>>,
    element_ty: &Value<'a>,
    mut it: impl ExactSizeIterator<Item = &'b ArrayDimension> + Clone,
) -> Result<Value<'a>, Box<dyn Error + Sync + Send>> {
    let dimension = it.next().expect("Must have one dimension at least");
    let len = dimension.len as u32;
    Ok(if it.len() == 0 {
        let begin = begin as u32 * len;
        Value::Array(
            Some(
                (begin..(begin + len))
                    .map(|i| mem::take(&mut values[i as usize]))
                    .collect(),
            ),
            Box::new(CollectionType {
                element: element_ty.clone(),
                len,
            }),
        )
    } else {
        let first = build_array(begin, values, element_ty, it.clone())?;
        let nested_ty = first.as_null();
        let elements = iter::chain(
            iter::once(Ok(first)),
            (1..len).map(|i| build_array(i as usize + begin, values, &nested_ty, it.clone())),
        )
        .collect::<Result<_, _>>()?;
        Value::Array(
            Some(elements),
            Box::new(CollectionType {
                element: nested_ty,
                len,
            }),
        )
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
            Some(Value::Array(Some(inner), ..) | Value::List(Some(inner), ..)) => {
                current_level = inner
            }
            _ => break,
        }
    }
    loop {
        match element_type {
            Value::Array(.., t) | Value::List(.., t) => element_type = &t.element,
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
            Value::Array(Some(inner), ..) | Value::List(Some(inner), ..) => {
                flatten_recursive(inner, collector);
            }
            Value::Array(None, ..) | Value::List(None, ..) => {}
            _ => collector.push(value),
        }
    }
}

pub fn postgres_type_to_value(ty: &Type) -> ValueOwned {
    let list = |element| Value::List(None, Box::new(CollectionType { element, len: 0 }));
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
        Type::BOOL_ARRAY => list(Value::Boolean(None)),
        Type::INT2_ARRAY => list(Value::Int16(None)),
        Type::INT4_ARRAY => list(Value::Int32(None)),
        Type::INT8_ARRAY => list(Value::Int64(None)),
        Type::FLOAT4_ARRAY => list(Value::Float32(None)),
        Type::FLOAT8_ARRAY => list(Value::Float64(None)),
        Type::NUMERIC_ARRAY => list(Value::Decimal(None, 0, 0)),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => list(Value::Varchar(None)),
        Type::BYTEA_ARRAY => list(Value::Blob(None)),
        Type::DATE_ARRAY => list(Value::Date(None)),
        Type::TIME_ARRAY => list(Value::Time(None)),
        Type::TIMESTAMP_ARRAY => list(Value::Timestamp(None)),
        Type::TIMESTAMPTZ_ARRAY => list(Value::TimestampWithTimezone(None)),
        Type::INTERVAL_ARRAY => list(Value::Interval(None)),
        Type::UUID_ARRAY => list(Value::Uuid(None)),
        Type::JSON_ARRAY => list(Value::Json(None)),
        Type::JSON => Value::Json(None),
        _ => Value::Null,
    }
}

pub fn value_to_postgres_type(value: &Value<'_>) -> Type {
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
        Value::Array(.., t) | Value::List(.., t) => match t.element {
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
            Value::Array(..) | Value::List(..) => value_to_postgres_type(&t.element),
            Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON_ARRAY,
            _ => Type::UNKNOWN,
        },
        Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON,
        _ => Type::UNKNOWN,
    }
}
