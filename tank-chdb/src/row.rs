use crate::batch::ChDBBatch;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};
use tank_core::{Row, RowOwned, Value};

/// A zero-copy row view borrowing a ClickHouse `Native` block shared with its siblings.
pub struct ChDBRow {
    pub(crate) batch: Arc<ChDBBatch>,
    pub(crate) row: usize,
}

impl Row for ChDBRow {
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
        self.batch
            .columns
            .get(index)?
            .extract(&self.batch, self.row)
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

impl Debug for ChDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChDBRow")
            .field("labels", &self.batch.labels)
            .field("len", &self.len())
            .finish()
    }
}
