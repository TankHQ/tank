use crate::util::extract_value;
use postgres_types::{FromSql, Type};
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
use tank_core::{Error as TankError, Result, Row, RowLabels, RowOwned, Value, stream::Stream};

struct RawCell<'a>(Option<&'a [u8]>);

impl<'a> FromSql<'a> for RawCell<'a> {
    #[inline(always)]
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(Some(raw)))
    }

    #[inline(always)]
    fn from_sql_null(_ty: &Type) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(None))
    }

    #[inline(always)]
    fn from_sql_nullable(
        _ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(raw))
    }

    #[inline(always)]
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// A native row view borrowing the connection's cached column labels.
pub struct PostgresRow<'a> {
    pub(crate) row: tokio_postgres::Row,
    pub(crate) labels: &'a RowLabels,
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
    fn decode_fallback<'a>(ty: &Type, raw: &'a [u8]) -> Option<Value<'a>> {
        extract_value(ty, Some(raw)).ok()
    }
}

impl Row for PostgresRow<'_> {
    #[inline(always)]
    fn names(&self) -> &[String] {
        self.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        min(self.labels.len(), self.row.len())
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        if index >= self.len() {
            return None;
        }
        let raw = match self.row.try_get::<usize, RawCell<'_>>(index) {
            Ok(RawCell(Some(raw))) => raw,
            Ok(RawCell(None)) => return Some(Cow::Owned(Value::Null)),
            Err(e) => {
                mem::forget(e);
                return None;
            }
        };
        // `BOOL` also places a 1-byte `Option<bool>` store at offset 1..2 of `Value`,
        // splitting the 3-byte 1..4 gap into `i8` + `i16` so LLVM SROA promotes both to SSA.
        match *self.row.columns()[index].type_() {
            Type::BOOL => match *raw {
                [b] => Some(Cow::Owned(Value::Boolean(Some(b != 0)))),
                _ => Some(Cow::Owned(Value::Null)),
            },
            Type::INT4 => match raw.try_into() {
                Ok(b) => Some(Cow::Owned(Value::Int32(Some(i32::from_be_bytes(b))))),
                Err(_) => Some(Cow::Owned(Value::Null)),
            },
            Type::VARCHAR | Type::TEXT => match std::str::from_utf8(raw) {
                Ok(text) => Some(Cow::Owned(Value::Varchar(Some(Cow::Borrowed(text))))),
                Err(_) => Some(Cow::Owned(Value::Null)),
            },
            ref ty => match Self::decode_fallback(ty, raw) {
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

/// A streaming view over a `tokio_postgres::RowStream` and shared column labels.
pub struct PostgresRows<'s> {
    first: Option<tokio_postgres::Row>,
    stream: tokio_postgres::RowStream,
    labels: &'s RowLabels,
}

impl<'s> PostgresRows<'s> {
    #[inline(always)]
    pub(crate) fn new(
        first: Option<tokio_postgres::Row>,
        stream: tokio_postgres::RowStream,
        labels: &'s RowLabels,
    ) -> Self {
        Self {
            first,
            stream,
            labels,
        }
    }

    #[cold]
    #[inline(never)]
    fn row_error(e: tokio_postgres::Error) -> TankError {
        let error = TankError::new(e);
        log::error!("{error:#}");
        error
    }

    #[inline(always)]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<PostgresRow<'s>>>> {
        if let Some(row) = self.first.take() {
            return Poll::Ready(Some(Ok(PostgresRow {
                row,
                labels: self.labels,
            })));
        }
        match unsafe { Pin::new_unchecked(&mut self.stream) }.poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => Poll::Ready(Some(Ok(PostgresRow {
                row,
                labels: self.labels,
            }))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Self::row_error(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream adapter that awaits the query execution future once, then yields rows from `PostgresRows`.
pub(crate) struct PostgresStream<'s, F> {
    fut: Option<F>,
    rows: Option<PostgresRows<'s>>,
}

impl<'s, F> PostgresStream<'s, F> {
    #[inline(always)]
    pub(crate) fn new(fut: F) -> Self {
        Self {
            fut: Some(fut),
            rows: None,
        }
    }

    #[cold]
    #[inline(never)]
    fn poll_init(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
        F: Future<Output = Result<PostgresRows<'s>>>,
    {
        let Some(fut) = self.fut.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let Poll::Ready(res) = unsafe { Pin::new_unchecked(fut) }.poll(cx) else {
            return Poll::Pending;
        };
        self.fut = None;
        self.rows = Some(res?);
        Poll::Ready(Ok(()))
    }
}

impl<'s, F> Stream for PostgresStream<'s, F>
where
    F: Future<Output = Result<PostgresRows<'s>>>,
{
    type Item = Result<PostgresRow<'s>>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.rows.is_none() {
            match this.poll_init(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        unsafe { this.rows.as_mut().unwrap_unchecked() }.poll_next(cx)
    }
}
