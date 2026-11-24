use crate::{column::Column, cursor::Cursor, native::NativeResult};
use std::{ptr, sync::Arc};
use tank_core::{Result, RowLabels};

/// A decoded ClickHouse `Native` block owning the underlying chunk buffer.
#[derive(Debug)]
pub(crate) struct ChDBBatch {
    result: Option<Arc<NativeResult>>,
    pub(crate) labels: RowLabels,
    pub(crate) columns: Vec<Column>,
    pub(crate) spans: Vec<(u32, u32)>,
    rows: usize,
}

impl ChDBBatch {
    /// Drops the underlying C++ chunk buffer while retaining vector capacities and labels.
    pub(crate) fn release(&mut self) {
        self.columns.clear();
        self.spans.clear();
        self.rows = 0;
        self.result = None;
    }

    fn clean_label(name: &str) -> &str {
        name.rsplit('.')
            .next()
            .unwrap_or(name)
            .trim_matches('`')
            .trim_matches('"')
    }

    /// Parses one ClickHouse `Native` block into `slot`, reusing buffers when unshared.
    pub(crate) fn parse_into(
        slot: &mut Option<Arc<Self>>,
        result: Arc<NativeResult>,
        offset: &mut usize,
    ) -> Result<usize> {
        let data = result.data()?;
        let mut cursor = Cursor::new(data, *offset);
        let num_cols = cursor.read_varuint()?;
        let rows = cursor.read_varuint()?;

        if let Some(existing) = slot.as_mut()
            && let Some(batch) = Arc::get_mut(existing)
        {
            batch.columns.clear();
            batch.spans.clear();
            batch.result = None;
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
            *offset = cursor.pos();
            batch.result = Some(result);
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
            result: Some(result),
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

unsafe impl Send for ChDBBatch {}
unsafe impl Sync for ChDBBatch {}
