use crate::interval_wrap::IntervalWrap;
use anyhow::anyhow;
use fallible_iterator::FallibleIterator;
use postgres_protocol::types::{ArrayDimension, array_from_sql};
use postgres_types::{FromSql, Kind, Type};
use rust_decimal::Decimal;
use std::{
    borrow::Cow,
    cmp::min,
    error::Error,
    fmt::{self, Debug, Formatter},
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use tank_core::{
    CollectionType, QueryResult, Result, Row, RowLabels, RowOwned, RowsAffected, Value, ValueOwned,
    stream::Stream,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;
use wtx::{
    codec::Decode,
    database::{
        Record as _, Records as _,
        client::postgres::{Postgres, PostgresDecodeWrapper, PostgresRecord, PostgresRecords, Ty},
    },
    misc::from_utf8_basic,
};

/// A native row view borrowing the connection's wire buffer and column metadata.
///
/// The record descriptor is resolved once per row; values decode on demand
/// without cloning the shared labels or copying text and binary payloads.
pub struct PostgresRow<'a> {
    record: PostgresRecord<'a, wtx::Error>,
    labels: &'a RowLabels,
}

impl Debug for PostgresRow<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresRow")
            .field("labels", &self.labels)
            .field("len", &self.len())
            .finish()
    }
}

impl PostgresRow<'_> {
    #[cold]
    #[inline(never)]
    fn decode_fallback<'a>(ty: Ty, raw: &'a [u8]) -> Option<Value<'a>> {
        let pg_ty = Type::from_oid(u32::from(ty)).unwrap_or(Type::UNKNOWN);
        Self::extract_value_pg(&pg_ty, Some(raw)).ok()
    }

    fn convert<'a, T: FromSql<'a>>(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> std::result::Result<Option<T>, Box<dyn Error + Sync + Send>> {
        Ok(match raw {
            Some(raw) => Some(T::from_sql(ty, raw)?),
            None => None,
        })
    }

    fn decode_wtx<'a, T: Decode<'a, Postgres<wtx::Error>>>(
        raw: Option<&'a [u8]>,
    ) -> std::result::Result<Option<T>, Box<dyn Error + Sync + Send>> {
        Ok(match raw {
            Some(raw) => Some(T::decode(&mut PostgresDecodeWrapper::new(
                raw,
                "",
                Ty::Any,
            ))?),
            None => None,
        })
    }

    fn extract_value_pg<'a>(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> std::result::Result<Value<'a>, Box<dyn Error + Sync + Send>> {
        let kind = ty.kind();
        Ok(match kind {
            Kind::Simple => match *ty {
                Type::BOOL => Value::Boolean(Self::decode_wtx::<bool>(raw)?),
                Type::CHAR => Value::Int8(Self::decode_wtx::<i8>(raw)?),
                Type::INT2 => Value::Int16(Self::decode_wtx::<i16>(raw)?),
                Type::INT4 => Value::Int32(Self::decode_wtx::<i32>(raw)?),
                Type::INT8 => Value::Int64(Self::decode_wtx::<i64>(raw)?),
                Type::FLOAT4 => Value::Float32(Self::decode_wtx::<f32>(raw)?),
                Type::FLOAT8 => Value::Float64(Self::decode_wtx::<f64>(raw)?),
                Type::NUMERIC => Value::Decimal(Self::decode_wtx::<Decimal>(raw)?, 0, 0),
                Type::OID => Value::UInt32(Self::convert::<u32>(ty, raw)?),
                Type::VARCHAR | Type::TEXT | Type::NAME | Type::BPCHAR | Type::JSON | Type::XML => {
                    Value::Varchar(Self::decode_wtx::<&str>(raw)?.map(Cow::Borrowed))
                }
                Type::BYTEA => Value::Blob(Self::decode_wtx::<&[u8]>(raw)?.map(Cow::Borrowed)),
                Type::DATE => Value::Date(Self::convert::<Date>(ty, raw)?),
                Type::TIME => Value::Time(Self::convert::<Time>(ty, raw)?),
                Type::TIMESTAMP => Value::Timestamp(Self::convert::<PrimitiveDateTime>(ty, raw)?),
                Type::TIMESTAMPTZ => {
                    Value::TimestampWithTimezone(Self::convert::<OffsetDateTime>(ty, raw)?)
                }
                Type::INTERVAL => {
                    Value::Interval(Self::convert::<IntervalWrap>(ty, raw)?.map(Into::into))
                }
                Type::UUID => Value::Uuid(Self::decode_wtx::<Uuid>(raw)?),
                Type::UNKNOWN => Value::Unknown(Self::decode_wtx::<&str>(raw)?.map(Cow::Borrowed)),
                _ => {
                    return Err(anyhow!("Unexpected {ty} variant for Kind::Simple").into());
                }
            },
            Kind::Array(inner_ty) => {
                let t = Self::extract_value_pg(inner_ty, None)?.into_owned();
                if let Some(raw) = raw {
                    let array = array_from_sql(raw)?;
                    let dimensions = array.dimensions().collect::<Vec<_>>()?;
                    if dimensions.is_empty() {
                        Value::Array(
                            Some(Vec::new().into()),
                            Box::new(CollectionType { element: t, len: 0 }),
                        )
                    } else {
                        let mut values = array
                            .values()
                            .map(|v| Self::extract_value_pg(inner_ty, v))
                            .collect::<Vec<_>>()?;
                        let mut cursor = 0;
                        Self::build_array(&mut cursor, &mut values, &t, dimensions.iter().cloned())?
                    }
                } else {
                    Value::List(None, Box::new(CollectionType { element: t, len: 0 }))
                }
            }
            Kind::Pseudo | Kind::Enum(_) => match raw {
                Some(raw) => {
                    Value::Unknown(Some(Cow::Borrowed(from_utf8_basic(raw).unwrap_or(""))))
                }
                None => Value::Unknown(None),
            },
            _ => return Err(anyhow!("Unexpected kind {kind:?}").into()),
        })
    }

    fn build_array<'a>(
        cursor: &mut usize,
        values: &mut Vec<Value<'a>>,
        element_ty: &ValueOwned,
        mut it: impl ExactSizeIterator<Item = ArrayDimension> + Clone,
    ) -> std::result::Result<Value<'a>, Box<dyn Error + Sync + Send>> {
        let dimension = it.next().expect("Must have one dimension at least");
        let len = dimension.len as u32;
        Ok(if it.len() == 0 {
            let begin = *cursor;
            let end = begin + len as usize;
            *cursor = end;
            Value::Array(
                Some((begin..end).map(|i| mem::take(&mut values[i])).collect()),
                Box::new(CollectionType {
                    element: element_ty.clone(),
                    len,
                }),
            )
        } else {
            let mut elements = Vec::with_capacity(len as usize);
            let mut nested_t = element_ty.clone();
            for _ in 0..len {
                let child = Self::build_array(cursor, values, element_ty, it.clone())?;
                nested_t = child.as_null();
                elements.push(child);
            }
            Value::Array(
                Some(elements.into_boxed_slice()),
                Box::new(CollectionType {
                    element: nested_t,
                    len,
                }),
            )
        })
    }
}

impl Row for PostgresRow<'_> {
    #[inline(always)]
    fn names(&self) -> &[String] {
        self.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        min(self.labels.len(), self.record.len())
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        if index >= self.len() {
            return None;
        }
        let Some(mut dw) = self.record.value(index) else {
            return Some(Cow::Owned(Value::Null));
        };
        // `Bool` also places a 1-byte `Option<bool>` store at offset 1..2 of `Value`,
        // splitting the 3-byte 1..4 gap into `i8` + `i16` so LLVM SROA promotes both to SSA.
        match *dw.ty() {
            Ty::Bool => match <bool as Decode<Postgres<wtx::Error>>>::decode(&mut dw) {
                Ok(v) => Some(Cow::Owned(Value::Boolean(Some(v)))),
                Err(e) => {
                    mem::forget(e);
                    Some(Cow::Owned(Value::Null))
                }
            },
            Ty::Int4 => match <i32 as Decode<Postgres<wtx::Error>>>::decode(&mut dw) {
                Ok(v) => Some(Cow::Owned(Value::Int32(Some(v)))),
                Err(e) => {
                    mem::forget(e);
                    Some(Cow::Owned(Value::Null))
                }
            },
            Ty::Varchar | Ty::Text => {
                match <&str as Decode<Postgres<wtx::Error>>>::decode(&mut dw) {
                    Ok(text) => Some(Cow::Owned(Value::Varchar(Some(Cow::Borrowed(text))))),
                    Err(e) => {
                        mem::forget(e);
                        Some(Cow::Owned(Value::Null))
                    }
                }
            }
            ty => match Self::decode_fallback(ty, dw.bytes()) {
                Some(v) => Some(Cow::Owned(v)),
                None => Some(Cow::Owned(Value::Null)),
            },
        }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned {
            names: self.labels.clone(),
            values: self
                .values()
                .map(|value| value.into_owned().into_owned())
                .collect(),
        }
    }
}

/// A lazy, borrowing view over a connection's result set.
///
/// The lifetime `'s` is the borrow of the connection; the view cannot outlive
/// it, which is what lets the rows borrow `wtx`'s read buffer. Native
/// [`PostgresRow`] views keep decoding monomorphised without `dyn` dispatch.
pub struct PostgresRows<'s> {
    /// The driver's borrowing view over the wire buffer.
    records: PostgresRecords<'s, wtx::Error>,
    index: usize,
    /// Column labels, borrowed from the connection.
    labels: &'s RowLabels,
}

impl<'s> PostgresRows<'s> {
    #[inline(always)]
    pub(crate) fn new(
        records: PostgresRecords<'s, wtx::Error>,
        labels: &'s RowLabels,
    ) -> PostgresRows<'s> {
        PostgresRows {
            records,
            index: 0,
            labels,
        }
    }
}

impl<'s> Iterator for PostgresRows<'s> {
    type Item = Result<PostgresRow<'s>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let record = self.records.get(self.index)?;
        self.index += 1;
        Some(Ok(PostgresRow {
            record,
            labels: self.labels,
        }))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.records.len() - self.index;
        (remaining, Some(remaining))
    }
}

/// Statement-level iterator over either `PostgresRows` or a single `RowsAffected`.
pub(crate) struct PostgresResults<'s> {
    rows: Option<PostgresRows<'s>>,
    affected: Option<RowsAffected>,
}

impl<'s> PostgresResults<'s> {
    #[inline(always)]
    pub(crate) fn rows(rows: PostgresRows<'s>) -> Self {
        Self {
            rows: Some(rows),
            affected: None,
        }
    }

    #[inline(always)]
    pub(crate) fn affected(affected: RowsAffected) -> Self {
        Self {
            rows: None,
            affected: Some(affected),
        }
    }
}

impl<'s> Iterator for PostgresResults<'s> {
    type Item = Result<QueryResult<PostgresRow<'s>>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.rows.as_mut().and_then(Iterator::next) {
            return Some(row.map(QueryResult::Row));
        }
        self.affected.take().map(|a| Ok(QueryResult::Affected(a)))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self
            .rows
            .as_ref()
            .map_or(0, |rows| rows.records.len() - rows.index)
            + usize::from(self.affected.is_some());
        (remaining, Some(remaining))
    }
}

/// Stream adapter that awaits the query execution future once, then yields items from `I`.
pub(crate) struct PostgresStream<F, I> {
    fut: Option<F>,
    iter: Option<I>,
}

impl<F, I> PostgresStream<F, I> {
    #[inline(always)]
    pub(crate) fn new(fut: F) -> Self {
        Self {
            fut: Some(fut),
            iter: None,
        }
    }

    // Return `Poll<Result<()>>` instead of `Poll<Option<Result<T>>>` so `poll_init`
    // has no `sret` pointer and only initializes `self.iter` once.
    #[cold]
    #[inline(never)]
    fn poll_init(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
        F: Future<Output = Result<I>>,
    {
        let Some(fut) = self.fut.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let Poll::Ready(res) = unsafe { Pin::new_unchecked(fut) }.poll(cx) else {
            return Poll::Pending;
        };
        self.fut = None;
        self.iter = Some(res?);
        Poll::Ready(Ok(()))
    }
}

impl<F, I, T> Stream for PostgresStream<F, I>
where
    F: Future<Output = Result<I>>,
    I: Iterator<Item = Result<T>>,
{
    type Item = Result<T>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.iter.is_none() {
            match this.poll_init(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(this.iter.as_mut().and_then(Iterator::next))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.iter {
            Some(iter) => iter.size_hint(),
            None => (0, None),
        }
    }
}
