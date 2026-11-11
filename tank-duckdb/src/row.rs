use crate::{column::DuckDBColumn, util::CBox};
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::CStr,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};
use tank_core::{Row, RowLabels, RowOwned, Value};

/// A data chunk shared by the rows decoded from it.
// Caching each column's `data` and `validity` pointers per chunk avoids three C FFI calls per cell in `DuckDBRow::value`.
#[derive(Debug)]
pub(crate) struct Batch {
    _chunk: CBox<duckdb_data_chunk>,
    labels: RowLabels,
    columns: Vec<DuckDBColumn>,
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}

impl Batch {
    /// The first chunk of a result.
    pub(crate) fn new(result: &mut duckdb_result, chunk: CBox<duckdb_data_chunk>) -> Self {
        unsafe {
            let columns = duckdb_column_count(result) as usize;
            Self {
                labels: (0..columns)
                    .map(|i| {
                        let ptr = duckdb_column_name(result, i as _);
                        if ptr.is_null() {
                            String::new()
                        } else {
                            CStr::from_ptr(ptr).to_string_lossy().into_owned()
                        }
                    })
                    .collect(),
                columns: (0..columns)
                    .map(|i| DuckDBColumn::new(duckdb_data_chunk_get_vector(*chunk, i as _)))
                    .collect(),
                _chunk: chunk,
            }
        }
    }

    /// The next chunk of the same result.
    pub(crate) fn next(&self, chunk: CBox<duckdb_data_chunk>) -> Self {
        Self {
            labels: self.labels.clone(),
            columns: self
                .columns
                .iter()
                .enumerate()
                .map(|(i, column)| {
                    column.rebind(unsafe { duckdb_data_chunk_get_vector(*chunk, i as _) })
                })
                .collect(),
            _chunk: chunk,
        }
    }
}

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
        unsafe { self.batch.columns.get(index)?.value(self.row) }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.labels.clone(),
            (0..self.len())
                .map(|i| {
                    unsafe { self.value(i).unwrap_unchecked() }
                        .into_owned()
                        .into_owned()
                })
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
