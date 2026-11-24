use crate::{
    column::Column,
    cursor::Cursor,
    ffi::{chdb_destroy_query_result, result_data},
};
use std::{ffi::c_void, mem, ptr, sync::Arc};
use tank_core::{Result, RowLabels};

/// A decoded ClickHouse `Native` block owning (or sharing) the underlying `chdb_result`.
#[derive(Debug)]
pub(crate) struct ChDBBatch {
    raw: *mut c_void,
    parent: Option<Arc<ChDBBatch>>,
    pub(crate) labels: RowLabels,
    pub(crate) columns: Vec<Column>,
    pub(crate) spans: Vec<(u32, u32)>,
    rows: usize,
}

impl ChDBBatch {
    #[inline(always)]
    pub(crate) fn raw_result(&self) -> *mut c_void {
        if !self.raw.is_null() {
            self.raw
        } else if let Some(parent) = self.parent.as_ref() {
            parent.raw_result()
        } else {
            ptr::null_mut()
        }
    }

    /// Drops the underlying C++ chunk buffer while retaining vector capacities and labels.
    pub(crate) fn release(&mut self) {
        self.columns.clear();
        self.spans.clear();
        self.rows = 0;
        self.parent = None;
        let raw = mem::replace(&mut self.raw, ptr::null_mut());
        if !raw.is_null() {
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

    /// Parses one ClickHouse `Native` block from a fresh `chdb_result` handle into `slot`.
    /// Takes ownership of `raw` and destroys it on error.
    pub(crate) fn parse_raw(
        slot: &mut Option<Arc<Self>>,
        raw: *mut c_void,
        offset: &mut usize,
    ) -> Result<usize> {
        let res = Self::parse_inner(slot, raw, None, offset);
        if res.is_err() && !raw.is_null() {
            unsafe { chdb_destroy_query_result(raw) };
        }
        res
    }

    /// Parses a subsequent `Native` block from the same multi-block `parent` buffer into `slot`.
    pub(crate) fn parse_next(
        slot: &mut Option<Arc<Self>>,
        parent: Arc<Self>,
        offset: &mut usize,
    ) -> Result<usize> {
        let raw = parent.raw_result();
        Self::parse_inner(slot, raw, Some(parent), offset)
    }

    fn parse_inner(
        slot: &mut Option<Arc<Self>>,
        data_handle: *mut c_void,
        parent: Option<Arc<Self>>,
        offset: &mut usize,
    ) -> Result<usize> {
        let data = unsafe { result_data(data_handle)? };
        let mut cursor = Cursor::new(data, *offset);
        let num_cols = cursor.read_varuint()?;
        let rows = cursor.read_varuint()?;
        let owned_raw = if parent.is_some() {
            ptr::null_mut()
        } else {
            data_handle
        };

        if let Some(existing) = slot.as_mut()
            && let Some(batch) = Arc::get_mut(existing)
        {
            batch.release();
            let labels = Self::parse_columns(
                num_cols,
                rows,
                &mut cursor,
                Some(&batch.labels),
                &mut batch.columns,
                &mut batch.spans,
            )?;
            if let Some(labels) = labels {
                batch.labels = labels;
            }
            batch.rows = rows;
            batch.raw = owned_raw;
            batch.parent = parent;
            *offset = cursor.pos();
            return Ok(rows);
        }

        let prev_labels = slot.as_ref().map(|b| &b.labels);
        let mut columns = Vec::with_capacity(num_cols);
        let mut spans = Vec::new();
        let new_labels = Self::parse_columns(
            num_cols,
            rows,
            &mut cursor,
            prev_labels,
            &mut columns,
            &mut spans,
        )?;
        let labels = match new_labels {
            Some(labels) => labels,
            None => prev_labels.cloned().unwrap_or_default(),
        };
        *offset = cursor.pos();
        *slot = Some(Arc::new(Self {
            raw: owned_raw,
            parent,
            labels,
            columns,
            spans,
            rows,
        }));
        Ok(rows)
    }

    fn parse_columns(
        num_cols: usize,
        rows: usize,
        cursor: &mut Cursor<'_>,
        prev_labels: Option<&RowLabels>,
        columns: &mut Vec<Column>,
        spans: &mut Vec<(u32, u32)>,
    ) -> Result<Option<RowLabels>> {
        columns.reserve(num_cols);
        let mut reuse_labels = prev_labels.is_some_and(|l| l.len() == num_cols);
        let mut names = if reuse_labels {
            Vec::new()
        } else {
            Vec::with_capacity(num_cols)
        };
        for col_idx in 0..num_cols {
            let raw_name = cursor.read_str()?;
            let clean = Self::clean_label(raw_name);
            if reuse_labels {
                if prev_labels
                    .and_then(|l| l.get(col_idx))
                    .is_none_or(|prev| prev != clean)
                {
                    reuse_labels = false;
                    names.reserve(num_cols);
                    if let Some(prev) = prev_labels {
                        names.extend(prev.iter().take(col_idx).cloned());
                    }
                    names.push(clean.to_owned());
                }
            } else {
                names.push(clean.to_owned());
            }
            let ty = cursor.read_str()?;
            columns.push(Column::parse(ty, rows, cursor, spans, ptr::null())?);
        }
        Ok((!reuse_labels).then(|| names.into()))
    }
}

// `raw` is a read-only chDB result buffer, and pointers inside `Column` borrow from `raw` or `parent`.
unsafe impl Send for ChDBBatch {}
unsafe impl Sync for ChDBBatch {}

impl Drop for ChDBBatch {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { chdb_destroy_query_result(self.raw) };
        }
    }
}
