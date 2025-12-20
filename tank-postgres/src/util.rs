use crate::{ValueWrap, interval_wrap::IntervalWrap};
use async_stream::try_stream;
use postgres_protocol::types::{ArrayDimension, array_from_sql};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;
use std::{error::Error, iter, mem, pin::pin};
use tank_core::{
    ErrorContext, Value,
    stream::{Stream, StreamExt},
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use tokio_postgres::{SimpleQueryMessage, fallible_iterator::FallibleIterator};
use uuid::Uuid;

pub(crate) fn row_to_tank_row(row: tokio_postgres::Row) -> tank_core::Result<tank_core::Row> {
    (0..row.len())
        .map(|i| match row.try_get::<_, ValueWrap>(i) {
            Ok(v) => Ok(v.0),
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
        .collect::<tank_core::Result<tank_core::Row>>()
}

pub(crate) fn simple_query_row_to_tank_row(
    row: tokio_postgres::SimpleQueryRow,
) -> tank_core::Result<tank_core::Row> {
    (0..row.len())
        .map(|i| match row.try_get(i) {
            Ok(Some(v)) => ValueWrap::from_sql(&Type::UNKNOWN, v.as_bytes())
                .map(|v| v.0)
                .map_err(|e| tank_core::Error::msg(format!("{:#}", e))),
            Ok(None) => Ok(Value::Null),
            Err(..) => {
                let col = &row.columns()[i];
                Err(tank_core::Error::msg(format!(
                    "Could not deserialize column {} `{}`",
                    i,
                    col.name(),
                )))
            }
        })
        .collect::<tank_core::Result<tank_core::Row>>()
}

pub(crate) fn stream_postgres_row_to_tank_row<V, R>(
    stream: impl AsyncFnOnce() -> tank_core::Result<V>,
) -> impl Stream<Item = tank_core::Result<R>>
where
    V: Stream<Item = Result<tokio_postgres::Row, tokio_postgres::Error>>,
    R: From<tank_core::RowLabeled>,
{
    try_stream! {
        let stream = stream().await?;
        let mut stream = pin!(stream);
        let mut labels: Option<tank_core::RowNames> = None;
        while let Some(row) = stream.next().await.transpose()? {
            let labels = labels.get_or_insert_with(|| {
                row.columns().iter().map(|c| c.name().to_string()).collect()
            });
            yield tank_core::RowLabeled {
                labels: labels.clone(),
                values: row_to_tank_row(row)?,
            }.into();
        }
    }
}

pub(crate) fn stream_postgres_simple_query_message_to_tank_query_result<V, R>(
    stream: impl AsyncFnOnce() -> tank_core::Result<V>,
) -> impl Stream<Item = tank_core::Result<R>>
where
    V: Stream<Item = Result<tokio_postgres::SimpleQueryMessage, tokio_postgres::Error>>,
    R: From<tank_core::QueryResult>,
{
    try_stream! {
        let stream = stream().await?;
        let mut stream = pin!(stream);
        let mut labels: Option<tank_core::RowNames> = None;
        let mut rows = false;
        while let Some(entry) = stream.next().await.transpose()? {
            match entry {
                SimpleQueryMessage::Row(row) => {
                    yield tank_core::QueryResult::Row(tank_core::RowLabeled {
                        labels: labels
                            .as_ref()
                            .filter(|v| v.len() == row.len())
                            .ok_or_else(|| {
                                tank_core::Error::msg(
                                    "Row columns names does not match the row currently received",
                                )
                            })?
                            .clone(),
                        values: simple_query_row_to_tank_row(row)?,
                    })
                    .into();
                }
                SimpleQueryMessage::CommandComplete(rows_affected) => {
                    if rows {
                        // After a set of rows, a CommandComplete is received, ignore it
                        rows = false;
                    } else {
                        yield tank_core::QueryResult::Affected(tank_core::RowsAffected {
                            rows_affected: Some(rows_affected),
                            ..Default::default()
                        })
                        .into();
                    }
                }
                SimpleQueryMessage::RowDescription(columns) => {
                    rows = true;
                    labels = Some(
                        columns
                            .into_iter()
                            .map(|col| col.name().into())
                            .collect::<tank_core::RowNames>(),
                    );
                    if columns.is_empty() {
                        log::warn!(
                            "The row description contains no columns, this can be expected but it can also be symthon of a wrong query"
                        )
                    }
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn extract_value(
    ty: &Type,
    raw: Option<&[u8]>,
) -> Result<Value, Box<dyn Error + Sync + Send>> {
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
                Value::Varchar(convert::<String>(ty, raw)?.map(Into::into))
            }
            Type::BYTEA => Value::Blob(convert::<Vec<u8>>(ty, raw)?.map(Into::into)),
            Type::DATE => Value::Date(convert::<Date>(ty, raw)?),
            Type::TIME => Value::Time(convert::<Time>(ty, raw)?),
            Type::TIMESTAMP => Value::Timestamp(convert::<PrimitiveDateTime>(ty, raw)?),
            Type::TIMESTAMPTZ => Value::TimestampWithTimezone(convert::<OffsetDateTime>(ty, raw)?),
            Type::INTERVAL => Value::Interval(convert::<IntervalWrap>(ty, raw)?.map(Into::into)),
            Type::UUID => Value::Uuid(convert::<Uuid>(ty, raw)?),
            Type::UNKNOWN => Value::Unknown(convert::<String>(ty, raw)?),
            _ => {
                return Err(tank_core::Error::msg(format!(
                    "Unexpected {ty} variant for Kind::Simple"
                ))
                .into());
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
                let first = build_array(0, &mut values, &ty, dimensions.iter())?;
                first
            } else {
                Value::List(None, Box::new(ty))
            }
        }
        _ => return Err(tank_core::Error::msg(format!("Unexpected kind {kind:?}")).into()),
    })
}

fn build_array<'a>(
    begin: usize,
    values: &mut Vec<Value>,
    element_ty: &Value,
    mut it: impl ExactSizeIterator<Item = &'a ArrayDimension> + Clone,
) -> Result<Value, Box<dyn Error + Sync + Send>> {
    let dimension = it.next().expect("Must have one dimension at least");
    let len = dimension.len as u32;
    Ok(if it.len() == 0 {
        let begin = begin as u32 * len;
        // Last array
        Value::Array(
            Some(
                (begin..(begin + len))
                    .map(|i| mem::take(&mut values[i as usize]))
                    .collect(),
            ),
            Box::new(element_ty.clone()),
            len,
        )
    } else {
        let first = build_array(begin, values, element_ty, it.clone())?;
        let nested_ty = first.as_null();
        let elements = iter::chain(
            iter::once(Ok(first)),
            (1..len).map(|i| build_array(i as usize + begin, values, &nested_ty, it.clone())),
        )
        .collect::<Result<_, _>>()?;
        Value::Array(Some(elements), Box::new(nested_ty), len)
    })
}
