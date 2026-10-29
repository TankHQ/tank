use std::{
    borrow::Cow,
    fmt,
    future::Future,
    pin::Pin,
    task::{self, Poll},
};
use tank_core::{
    QueryResult, Result, Row, RowLabels, RowOwned, RowsAffected, Value, stream::Stream,
};
use wtx::database::Record as _;
use wtx::database::Records as _;
use wtx::database::client::postgres::{PostgresRecord, PostgresRecords};

/// Reusable per-statement buffers owned by a connection.
///
/// A connection runs one query at a time, so the metadata of the previous query
/// is dead by the time the next one starts. Repeated statements reuse the
/// column labels and decoder capacity instead of reallocating metadata.
///
/// Unlike an arena-based design this struct does **not** copy the row payloads.
/// Decoding is lazy: each value is decoded on demand, straight out of `wtx`'s
/// read buffer.
#[derive(Default, Debug)]
pub(crate) struct PostgresScratch {
    /// Per-column decoders of the current statement, resolved once per query.
    pub(crate) decoders: Vec<crate::util::ColumnDecoder>,
    /// Labels of the current statement, reused while consecutive queries share
    /// the same column set (the common case for a prepared statement).
    pub(crate) labels: RowLabels,
}

impl PostgresScratch {
    /// Resolves the labels and per-column decoders of a result set in place.
    pub(crate) fn prepare<E>(&mut self, records: &PostgresRecords<'_, E>)
    where
        E: From<wtx::Error>,
    {
        self.decoders.clear();
        self.decoders.extend(
            records
                .columns()
                .into_iter()
                .flatten()
                .map(|(_, ty)| crate::util::ColumnDecoder::from_ty(ty)),
        );
        if self.decoders.len() != self.labels.len()
            || !records
                .columns()
                .into_iter()
                .flatten()
                .zip(self.labels.iter())
                .all(|((name, _), label)| name == label)
        {
            self.labels = records
                .columns()
                .into_iter()
                .flatten()
                .map(|(name, _)| name.to_string())
                .collect();
        }
    }
}

/// A native row view borrowing the connection's wire buffer and column metadata.
///
/// The record descriptor is resolved once per row; values decode on demand
/// without cloning the shared labels or copying text and binary payloads.
pub struct PostgresRow<'a> {
    record: PostgresRecord<'a, wtx::Error>,
    labels: &'a RowLabels,
    decoders: &'a [crate::util::ColumnDecoder],
}

impl fmt::Debug for PostgresRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresRow")
            .field("labels", &self.labels)
            .field("len", &self.decoders.len())
            .finish()
    }
}

impl Row for PostgresRow<'_> {
    #[inline(always)]
    fn names(&self) -> &[String] {
        self.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.decoders.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        // Missing cells and malformed wire data retain the lazy decoder's
        // NULL fallback; only an out-of-range column returns None.
        self.decoders
            .get(index)?
            .value(self.record.value(index).map(|v| v.bytes()))
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
    /// Column labels, borrowed from the connection's reusable scratch.
    labels: &'s RowLabels,
    /// Per-column decoders, borrowed from the connection's reusable scratch.
    decoders: &'s [crate::util::ColumnDecoder],
}

impl<'s> PostgresRows<'s> {
    /// Builds a lazy single-statement view borrowing the scratch.
    pub(crate) fn new(
        records: PostgresRecords<'s, wtx::Error>,
        labels: &'s RowLabels,
        decoders: &'s [crate::util::ColumnDecoder],
    ) -> PostgresRows<'s> {
        PostgresRows {
            records,
            index: 0,
            labels,
            decoders,
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
            decoders: self.decoders,
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
}

impl<F, I, T> Stream for PostgresStream<F, I>
where
    F: Future<Output = Result<I>>,
    I: Iterator<Item = Result<T>>,
{
    type Item = Result<T>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        // Return `Poll<Result<Option<T>>>` instead of `Poll<Option<Result<T>>>` so `poll_init`
        // does not share `poll_next`'s `sret` slot and block LLVM SROA on the hot row return.
        #[cold]
        #[inline(never)]
        fn poll_init<F, I, T>(
            this: &mut PostgresStream<F, I>,
            cx: &mut task::Context<'_>,
        ) -> Poll<Result<Option<T>>>
        where
            F: Future<Output = Result<I>>,
            I: Iterator<Item = Result<T>>,
        {
            let Some(fut) = this.fut.as_mut() else {
                return Poll::Ready(Ok(None));
            };
            let Poll::Ready(res) = unsafe { Pin::new_unchecked(fut) }.poll(cx) else {
                return Poll::Pending;
            };
            this.fut = None;
            let mut iter = res?;
            let first = iter.next().transpose()?;
            this.iter = Some(iter);
            Poll::Ready(Ok(first))
        }
        let this = unsafe { self.get_unchecked_mut() };
        if let Some(item) = this.iter.as_mut().and_then(Iterator::next) {
            Poll::Ready(Some(item))
        } else {
            match poll_init(this, cx) {
                Poll::Ready(res) => Poll::Ready(res.transpose()),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.iter {
            Some(iter) => iter.size_hint(),
            None => (0, None),
        }
    }
}

