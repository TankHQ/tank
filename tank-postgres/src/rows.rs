use tank_core::{
    QueryResult, Result, Results, Row, RowLabels, RowValues, RowValuesSource, Rows, RowsAffected,
    Value, stream::Stream,
};
use wtx::database::Record as _;
use wtx::database::Records as _;
use wtx::database::client::postgres::{PostgresRecord, PostgresRecords};

/// Reusable per-statement buffers owned by a connection.
///
/// A connection runs one query at a time, so the metadata of the previous query
/// is dead by the time the next one starts. Keeping it here means a repeated
/// prepared statement performs **zero allocations per query and zero per row**:
/// the column labels and per-column decoders are reused in place.
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
        let Some(record) = records.get(0) else {
            return Ok(());
        };
        for (_, ty) in record.columns() {
            self.decoders
                .push(crate::util::ColumnDecoder::from_type(
                    &crate::util::wtx_ty_to_pg_type(ty),
                ));
        }
        let cached = &self.cached_labels;
        let matches = record.columns().zip(cached.iter()).all(|((name, _), label)| name == label)
            && record.columns().count() == cached.len();
        if !matches {
            self.cached_labels = record.columns().map(|(name, _)| name.to_string()).collect();
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

/// A lazy, borrowing view over a connection's result set.
///
/// The lifetime `'s` is the borrow of the connection; the view cannot outlive
/// it, which is what lets the rows borrow `wtx`'s read buffer. The view is its
/// own [`RowValuesSource`], so decoding is monomorphised — no `dyn` dispatch.
pub struct PostgresResults<'s> {
    /// The per-row records, built once per query so a cell lookup is an index.
    records: Vec<PostgresRecord<'s, wtx::Error>>,
    /// Column labels, borrowed from the connection's reusable scratch.
    labels: &'s RowLabels,
    /// Per-column decoders, borrowed from the connection's reusable scratch.
    decoders: &'s [crate::util::ColumnDecoder],
    /// The ordered rows/affected-counts of the statement. A statement yields
    /// either rows or a single affected count, never both.
    affected: Option<RowsAffected>,
}

impl<'s> PostgresResults<'s> {
    /// Builds a lazy single-statement view borrowing the scratch.
    pub(crate) fn new(
        records: &PostgresRecords<'s, wtx::Error>,
        labels: &'s RowLabels,
        decoders: &'s [crate::util::ColumnDecoder],
    ) -> PostgresResults<'s> {
        let rows: Vec<PostgresRecord<'s, wtx::Error>> =
            (0..records.len()).filter_map(|i| records.get(i)).collect();
        let affected = if rows.is_empty() {
            Some(RowsAffected {
                rows_affected: Some(records.rows_affected()),
                last_affected_id: None,
            })
        } else {
            None
        };
        PostgresResults {
            records: rows,
            labels,
            decoders,
            affected,
        }
    }
}

impl RowValuesSource for PostgresResults<'_> {
    #[inline]
    fn row_len(&self, _row: usize) -> usize {
        self.decoders.len()
    }

    #[inline]
    fn value(&self, row: usize, col: usize) -> Option<Value<'_>> {
        let decoder = self.decoders.get(col)?;
        let record = self.records.get(row)?;
        // A NULL (or missing) cell decodes to `Value::Null`; only an
        // out-of-range index returns `None`.
        let Some(wrapper) = record.value(col) else {
            return Some(Value::Null);
        };
        // Malformed wire data cannot occur against a conforming server; degrade
        // to NULL rather than failing an on-demand decode.
        Some(
            decoder
                .decode(Some(wrapper.bytes()))
                .unwrap_or(Value::Null),
        )
    }
}

impl Rows for PostgresResults<'_> {
    type Source = Self;

    fn len(&self) -> usize {
        self.records.len()
    }

    fn row(&self, index: usize) -> Option<Result<Row<'_, Self::Source>>> {
        if index >= self.records.len() {
            return None;
        }
        Some(Ok(Row {
            labels: self.labels.clone(),
            values: RowValues::Lazy { source: self, row: index },
        }))
    }
}

impl Results for PostgresResults<'_> {
    fn results(&self) -> impl Stream<Item = Result<QueryResult<'_, Self::Source>>> + Send {
        // A statement yields either rows or a single affected count.
        let affected = self.affected;
        let rows = (0..self.records.len()).map(move |index| {
            Ok(QueryResult::Row(Row {
                labels: self.labels.clone(),
                values: RowValues::Lazy { source: self, row: index },
            }))
        });
        let tail = affected
            .into_iter()
            .map(|affected| Ok(QueryResult::Affected(affected)));
        tank_core::stream::iter(rows.chain(tail))
    }
}
