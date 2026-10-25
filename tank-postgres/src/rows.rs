use std::{borrow::Cow, fmt};
use tank_core::{QueryResult, Result, Row, RowLabels, RowOwned, Value};
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
    decoders: Vec<crate::util::ColumnDecoder>,
    /// Labels of the current statement, reused while consecutive queries share
    /// the same column set (the common case for a prepared statement).
    cached_labels: RowLabels,
}

impl PostgresScratch {
    /// Resolves the labels and per-column decoders of a result set in place.
    pub(crate) fn prepare<E>(&mut self, records: &PostgresRecords<'_, E>) -> Result<()>
    where
        E: From<wtx::Error>,
    {
        self.decoders.clear();
        for (_, ty) in records.columns().into_iter().flatten() {
            self.decoders.push(crate::util::ColumnDecoder::from_type(
                &crate::util::wtx_ty_to_pg_type(ty),
            ));
        }
        let cached = &self.cached_labels;
        let matches = records
            .columns()
            .into_iter()
            .flatten()
            .zip(cached.iter())
            .all(|((name, _), label)| name == label)
            && records.columns().into_iter().flatten().count() == cached.len();
        if !matches {
            self.cached_labels = records
                .columns()
                .into_iter()
                .flatten()
                .map(|(name, _)| name.to_string())
                .collect();
        }
        Ok(())
    }

    /// The labels resolved by the last [`prepare`](Self::prepare).
    pub(crate) fn labels(&self) -> &RowLabels {
        &self.cached_labels
    }

    /// The decoders resolved by the last [`prepare`](Self::prepare).
    pub(crate) fn decoders(&self) -> &[crate::util::ColumnDecoder] {
        &self.decoders
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
    fn names(&self) -> &[String] {
        self.labels
    }

    fn len(&self) -> usize {
        self.decoders.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        let decoder = self.decoders.get(index)?;
        // Missing cells and malformed wire data retain the lazy decoder's
        // NULL fallback; only an out-of-range column returns None.
        Some(Cow::Owned(match self.record.value(index) {
            Some(wrapper) => decoder.value(wrapper.bytes()),
            None => Value::Null,
        }))
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.records.len() - self.index;
        (remaining, Some(remaining))
    }
}

/// The statement-level results of a Postgres query.
///
/// A single statement yields either a row set or an affected count, never both.
pub struct PostgresResults<'s> {
    result: Option<Result<QueryResult<PostgresRows<'s>>>>,
}

impl<'s> PostgresResults<'s> {
    /// Builds the one-shot results from the resolved row set.
    pub(crate) fn new(result: Result<QueryResult<PostgresRows<'s>>>) -> Self {
        Self { result: Some(result) }
    }
}

impl<'s> Iterator for PostgresResults<'s> {
    type Item = Result<QueryResult<PostgresRows<'s>>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.result.take()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.result.is_some());
        (remaining, Some(remaining))
    }
}

