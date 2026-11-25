use crate::{CBox, column::Column};
use libduckdb_sys::*;
use std::{ffi::CStr, sync::Arc};
use tank_core::{RowLabels, char_ptr_to_string};

/// A data chunk shared by the rows decoded from it.
#[derive(Debug)]
pub(crate) struct Batch {
    chunk: CBox<duckdb_data_chunk>,
    pub(crate) labels: RowLabels,
    pub(crate) columns: Vec<Column>,
}

impl Batch {
    /// Loads `chunk` into `slot`, reusing the allocation and labels when no prior rows survive.
    pub(crate) fn load(
        slot: &mut Option<Arc<Self>>,
        result: &mut duckdb_result,
        chunk: CBox<duckdb_data_chunk>,
        same_result: bool,
    ) {
        if let Some(batch) = slot.as_mut()
            && let Some(inner) = Arc::get_mut(batch)
        {
            if same_result {
                for (i, column) in inner.columns.iter_mut().enumerate() {
                    column.rebind(unsafe { duckdb_data_chunk_get_vector(*chunk, i as _) });
                }
                inner.chunk = chunk;
            } else {
                inner.reset(result, chunk);
            }
            return;
        }
        let batch = if same_result && let Some(prev) = slot.as_ref() {
            Self {
                labels: prev.labels.clone(),
                columns: prev
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, &column)| {
                        let mut column = column;
                        column.rebind(unsafe { duckdb_data_chunk_get_vector(*chunk, i as _) });
                        column
                    })
                    .collect(),
                chunk,
            }
        } else {
            let mut batch = Self {
                chunk: CBox::null(),
                labels: RowLabels::default(),
                columns: Vec::new(),
            };
            batch.reset(result, chunk);
            batch
        };
        *slot = Some(Arc::new(batch));
    }

    fn reset(&mut self, result: &mut duckdb_result, chunk: CBox<duckdb_data_chunk>) {
        unsafe {
            let count = duckdb_column_count(result) as usize;
            let same_labels = self.labels.len() == count
                && self.labels.iter().enumerate().all(|(i, prev)| {
                    let ptr = duckdb_column_name(result, i as _);
                    let name = if ptr.is_null() {
                        b""
                    } else {
                        CStr::from_ptr(ptr).to_bytes()
                    };
                    prev.as_bytes() == name
                });
            if !same_labels {
                self.labels = (0..count)
                    .map(|i| char_ptr_to_string(duckdb_column_name(result, i as _)))
                    .collect();
            }
            self.columns.clear();
            self.columns.extend(
                (0..count).map(|i| Column::new(duckdb_data_chunk_get_vector(*chunk, i as _))),
            );
            self.chunk = chunk;
        }
    }

    /// Drops the active chunk while retaining cached labels and column vector capacity.
    pub(crate) fn clear(&mut self) {
        self.columns.clear();
        self.chunk = CBox::null();
    }
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}
