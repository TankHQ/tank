use crate::{
    NoSource, QueryResult, Result, Row, RowLabels, RowValues, RowValuesSource, RowsAffected, Value,
    ValueOwned,
};
use futures::{Stream, StreamExt, stream};
use std::ops::Range;

/// Materialized rows of a query. Provides a borrowing stream.
///
/// Each driver names its own [`RowValuesSource`] via the [`Source`](Self::Source)
/// associated type, so a driver that decodes lazily gets a monomorphised
/// (non-`dyn`) decode path.
pub trait Rows: Send + Sync {
    /// The row-values source produced by [`row`](Self::row).
    type Source: RowValuesSource + ?Sized;

    /// Number of rows in the result set (affected counts are not rows).
    fn len(&self) -> usize;

    /// True if there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows the `index`-th row (affected counts are skipped).
    ///
    /// The returned row borrows from `self`, the caller must not outlive it.
    fn row(&self, index: usize) -> Option<Result<Row<'_, Self::Source>>>;

    /// Streams the rows, each borrowing from `self`.
    fn rows(&self) -> impl Stream<Item = Result<Row<'_, Self::Source>>> + Send {
        stream::iter((0..self.len()).filter_map(|i| self.row(i)))
    }
}

/// Materialized results (QueryResult::Row or QueryResult::Affected) of a query.
/// Provides a borrowing stream.
pub trait Results: Rows {
    /// Streams the query results, each borrowing from `self`.
    fn results(&self) -> impl Stream<Item = Result<QueryResult<'_, Self::Source>>> + Send;
}

/// One ordered entry of an [`OwnedResults`] bundle.
#[derive(Clone, Copy, Debug)]
pub enum OwnedItem {
    /// The `index`-th row (into the spans arena).
    Row(usize),
    /// A modifying statement's affected-row count.
    Affected(RowsAffected),
}

/// A fully owning [`Results`] implementation.
#[derive(Default, Debug)]
pub struct OwnedResults {
    arena: Box<[ValueOwned]>,
    spans: Box<[Range<usize>]>,
    items: Box<[OwnedItem]>,
    /// Labels of the row at the same index in `spans`.
    labels: Box<[RowLabels]>,
}

impl OwnedResults {
    /// Creates an empty bundle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Collects a query-result stream into a bundle, flattening all rows into a single arena.
    /// Convenience for drivers whose `run` yields `QueryResult`s.
    pub async fn from_query_result_stream(
        stream: impl Stream<Item = Result<QueryResult<'_, NoSource>>>,
    ) -> Result<Self> {
        let mut arena: Vec<ValueOwned> = Vec::new();
        let mut spans: Vec<Range<usize>> = Vec::new();
        let mut items: Vec<OwnedItem> = Vec::new();
        let mut labels: Vec<RowLabels> = Vec::new();
        let mut stream = std::pin::pin!(stream);
        while let Some(item) = stream.next().await {
            match item? {
                QueryResult::Row(row) => {
                    let start = arena.len();
                    arena.extend(row.values.into_owned().iter().map(Value::into_owned));
                    spans.push(start..arena.len());
                    labels.push(row.labels);
                    items.push(OwnedItem::Row(spans.len() - 1));
                }
                QueryResult::Affected(affected) => items.push(OwnedItem::Affected(affected)),
            }
        }
        Ok(Self {
            arena: arena.into_boxed_slice(),
            spans: spans.into_boxed_slice(),
            items: items.into_boxed_slice(),
            labels: labels.into_boxed_slice(),
        })
    }
}

impl Rows for OwnedResults {
    type Source = NoSource;

    fn len(&self) -> usize {
        self.spans.len()
    }

    fn row(&self, index: usize) -> Option<Result<Row<'_, Self::Source>>> {
        let span = self.spans.get(index)?;
        Some(Ok(Row {
            labels: self.labels.get(index)?.clone(),
            values: RowValues::from(&self.arena[span.clone()]),
        }))
    }
}

impl Results for OwnedResults {
    fn results(&self) -> impl Stream<Item = Result<QueryResult<'_, Self::Source>>> + Send {
        let arena = &self.arena;
        let spans = &self.spans;
        let labels = &self.labels;
        stream::iter(self.items.iter().map(move |item| match item {
            OwnedItem::Row(index) => Ok(QueryResult::Row(Row {
                labels: labels[*index].clone(),
                values: RowValues::from(&arena[spans[*index].clone()]),
            })),
            OwnedItem::Affected(affected) => Ok(QueryResult::Affected(*affected)),
        }))
    }
}
