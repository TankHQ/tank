use crate::batch::Batch;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};
use tank_core::{Row, RowOwned, Value};

/// A native row view borrowing a DuckDB data chunk shared with its siblings.
pub struct DuckDBRow {
    pub(crate) batch: Arc<Batch>,
    pub(crate) row: usize,
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.columns.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        self.batch.columns.get(index)?.extract(self.row)
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.labels.clone(),
            self.values()
                .map(|value| value.into_owned().into_owned())
                .collect(),
        )
    }
}

impl Debug for DuckDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBRow")
            .field("labels", &self.batch.labels)
            .field("len", &self.len())
            .finish()
    }
}
