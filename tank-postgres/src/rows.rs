use crate::ValueWrap;
use std::borrow::Cow;
use std::ops::Range;
use tank_core::{
    QueryResult, Result, Results, Row, RowLabels, Rows, RowsAffected, Value, ValueOwned,
    stream::Stream,
};

/// A materialized Postgres result set.
///
/// The driver rows are kept alive in a heap-stable `Box<[tokio_postgres::Row]>`.
/// All values of all rows are decoded **once** into a single flat arena
/// (`Box<[Value]>`) with one allocation for the whole result set; per-row spans
/// index into it. [`Rows::row`] therefore returns a *borrowed* slice of the
/// arena — **zero allocation per row**.
///
/// # Self-reference
///
/// The arena's `Value`s borrow text/binary payloads from the driver rows, which
/// live in the same struct. This is sound because:
/// * the rows are boxed, so their heap addresses are stable;
/// * `arena` and `spans` are declared **before** `rows`, so they are dropped
///   first and can never outlive the borrowed data;
/// * nothing exposes the arena with a lifetime beyond `&self`.
pub struct PostgresResults {
    arena: Box<[ValueOwned]>,
    spans: Box<[Range<usize>]>,
    items: Box<[Item]>,
    labels: Box<[RowLabels]>,
    /// Keeps the driver rows alive: the arena's `Value`s borrow from them.
    /// Must be declared **after** `arena`/`spans` so it is dropped last.
    #[allow(dead_code)]
    rows: Box<[tokio_postgres::Row]>,
}

/// One ordered entry of a [`PostgresResults`] bundle.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Item {
    Row(usize),
    Affected(RowsAffected),
}

impl PostgresResults {
    /// Decodes `rows` into a flat borrowed arena.
    ///
    /// `affected` is emitted as a single [`QueryResult::Affected`] item when no
    /// rows were produced (DML/DDL statements).
    pub(crate) fn new(rows: Vec<tokio_postgres::Row>, affected: Option<u64>) -> Result<Self> {
        let labels: Box<[RowLabels]> = rows
            .first()
            .map(|r| {
                let labels: RowLabels =
                    r.columns().iter().map(|c| c.name().to_string()).collect();
                vec![labels; rows.len()].into_boxed_slice()
            })
            .unwrap_or_default();
        let columns = rows.first().map(|r| r.len()).unwrap_or(0);
        let mut arena: Vec<ValueOwned> = Vec::with_capacity(rows.len() * columns);
        let mut spans: Vec<Range<usize>> = Vec::with_capacity(rows.len());
        let mut items: Vec<Item> = Vec::with_capacity(rows.len().max(1));
        for row in &rows {
            let start = arena.len();
            for i in 0..row.len() {
                // SAFETY: `row` borrows from `rows`, which is moved into the
                // returned struct *after* the arena is built and lives at a
                // stable heap address (it is boxed below). The arena is dropped
                // before `rows` (declaration order), so the extended lifetime is
                // never observed beyond the driver rows' lifetime.
                let value: ValueOwned = unsafe { std::mem::transmute(decode_value(row, i)?) };
                arena.push(value);
            }
            spans.push(start..arena.len());
            items.push(Item::Row(spans.len() - 1));
        }
        if items.is_empty()
            && let Some(rows_affected) = affected
        {
            items.push(Item::Affected(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            }));
        }
        Ok(Self {
            arena: arena.into_boxed_slice(),
            spans: spans.into_boxed_slice(),
            items: items.into_boxed_slice(),
            labels,
            rows: rows.into_boxed_slice(),
        })
    }

    /// Builds a bundle from already-decoded owning parts.
    ///
    /// Used by the simple-protocol path, where values cannot borrow from the
    /// driver rows (they are decoded from text and owned).
    pub(crate) fn from_parts(
        arena: Vec<ValueOwned>,
        spans: Vec<Range<usize>>,
        items: Vec<Item>,
        labels: Vec<RowLabels>,
    ) -> Self {
        Self {
            arena: arena.into_boxed_slice(),
            spans: spans.into_boxed_slice(),
            items: items.into_boxed_slice(),
            labels: labels.into_boxed_slice(),
            rows: Vec::new().into_boxed_slice(),
        }
    }

    /// Labels shared by every row in this result set.
    pub fn labels(&self) -> &[String] {
        self.labels.first().map(|v| v.as_ref()).unwrap_or_default()
    }
}

/// Decodes column `i` of a driver row, borrowing where possible.
fn decode_value<'r>(row: &'r tokio_postgres::Row, i: usize) -> Result<Value<'r>> {
    match row.try_get::<_, ValueWrap>(i) {
        Ok(w) => Ok(match w.0 {
            Cow::Borrowed(v) => v.clone(),
            Cow::Owned(v) => v,
        }),
        Err(e) => {
            let col = &row.columns()[i];
            Err(tank_core::Error::new(e).context(format!(
                "Unknown type {} for column {} (index: {})",
                col.type_(),
                col.name(),
                i,
            )))
        }
    }
}

impl Rows for PostgresResults {
    fn len(&self) -> usize {
        self.spans.len()
    }

    fn row(&self, index: usize) -> Option<Result<Row<'_>>> {
        let span = self.spans.get(index)?;
        Some(Ok(Row {
            labels: self.labels.get(index)?.clone(),
            values: Cow::Borrowed(&self.arena[span.clone()]),
        }))
    }
}

impl Results for PostgresResults {
    fn results(&self) -> impl Stream<Item = Result<QueryResult<'_>>> + Send {
        let labels = &self.labels;
        let arena = &self.arena;
        let spans = &self.spans;
        tank_core::stream::iter(self.items.iter().map(move |item| match item {
            Item::Row(index) => Ok(QueryResult::Row(Row {
                labels: labels[*index].clone(),
                values: Cow::Borrowed(&arena[spans[*index].clone()]),
            })),
            Item::Affected(affected) => Ok(QueryResult::Affected(*affected)),
        }))
    }
}
