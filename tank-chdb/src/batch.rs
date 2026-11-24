use crate::{
    column::Column,
    cursor::Cursor,
    ffi::{chdb_destroy_query_result, result_data},
};
use anyhow::anyhow;
use std::{ffi::c_void, mem, ptr, sync::Arc};
use tank_core::{Result, RowLabels};

/// A decoded ClickHouse `Native` block owning (or sharing via `parent`) a `chdb_result`.
#[derive(Debug)]
pub(crate) struct ChDBBatch {
    raw: *mut c_void,
    parent: Option<Arc<ChDBBatch>>,
    pub(crate) labels: RowLabels,
    pub(crate) columns: Vec<Column>,
    pub(crate) spans: Vec<(u32, u32)>,
}

impl ChDBBatch {
    #[inline(always)]
    pub(crate) fn raw(&self) -> *mut c_void {
        self.raw
    }

    /// Drops the underlying C++ chunk buffer while retaining vector capacities and labels.
    pub(crate) fn release(&mut self) {
        self.columns.clear();
        self.spans.clear();
        let raw = mem::replace(&mut self.raw, ptr::null_mut());
        let owned = self.parent.take().is_none();
        if owned && !raw.is_null() {
            unsafe { chdb_destroy_query_result(raw) };
        }
    }

    fn clean_label(name: &str) -> &str {
        name.rsplit('.')
            .next()
            .unwrap_or(name)
            .trim_matches('`')
            .trim_matches('"')
    }

    /// Parses one ClickHouse `Native` block from `raw` into `slot`, reusing buffers when unshared.
    /// When `parent` is `None`, takes ownership of `raw` and destroys it on error.
    pub(crate) fn parse(
        slot: &mut Option<Arc<Self>>,
        raw: *mut c_void,
        parent: Option<Arc<Self>>,
        offset: &mut usize,
    ) -> Result<usize> {
        let owns_raw = parent.is_none();
        let res = (|| -> Result<usize> {
            let data = unsafe { result_data(raw)? };
            let mut cursor = Cursor::new(data, *offset);
            let num_cols = cursor.read_varuint()?;
            let rows = cursor.read_varuint()?;

            if slot.as_mut().and_then(Arc::get_mut).is_none() {
                let labels = slot.as_ref().map(|b| b.labels.clone()).unwrap_or_default();
                *slot = Some(Arc::new(Self {
                    raw: ptr::null_mut(),
                    parent: None,
                    labels,
                    columns: Vec::with_capacity(num_cols),
                    spans: Vec::new(),
                }));
            }
            let Some(batch) = slot.as_mut().and_then(Arc::get_mut) else {
                return Err(anyhow!("ChDB batch slot unexpectedly shared"));
            };
            batch.release();
            batch.columns.reserve(num_cols);

            let mut reuse_labels = batch.labels.len() == num_cols;
            let mut names = Vec::new();
            for col_idx in 0..num_cols {
                let clean = Self::clean_label(cursor.read_str()?);
                if reuse_labels {
                    if batch.labels.get(col_idx).is_none_or(|prev| prev != clean) {
                        reuse_labels = false;
                        names.reserve(num_cols);
                        names.extend(batch.labels.iter().take(col_idx).cloned());
                        names.push(clean.to_owned());
                    }
                } else {
                    names.push(clean.to_owned());
                }
                let ty = cursor.read_str()?;
                batch.columns.push(Column::parse(
                    ty,
                    rows,
                    &mut cursor,
                    &mut batch.spans,
                    ptr::null(),
                )?);
            }
            if !reuse_labels {
                batch.labels = names.into();
            }
            batch.raw = raw;
            batch.parent = parent;
            *offset = cursor.pos();
            Ok(rows)
        })();
        if res.is_err() && owns_raw && !raw.is_null() {
            unsafe { chdb_destroy_query_result(raw) };
        }
        res
    }
}

// `raw` is a read-only chDB result buffer, and pointers inside `Column` borrow from it.
unsafe impl Send for ChDBBatch {}
unsafe impl Sync for ChDBBatch {}

impl Drop for ChDBBatch {
    fn drop(&mut self) {
        if self.parent.is_none() && !self.raw.is_null() {
            unsafe { chdb_destroy_query_result(self.raw) };
        }
    }
}
