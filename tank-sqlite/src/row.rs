use crate::RawDb;
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{borrow::Cow, ffi::c_int, hint, ops::Range, slice, str, sync::Arc};
use tank_core::{Result, Row, RowLabels, RowOwned, Value};

#[derive(Debug)]
enum Cell {
    Null,
    Integer(i64),
    Float(f64),
    Text(Range<usize>),
    Blob(Range<usize>),
}

#[derive(Debug)]
struct SQLiteRowData {
    labels: RowLabels,
    cells: Vec<Cell>,
    bytes: Vec<u8>,
}

impl SQLiteRowData {
    fn new(labels: RowLabels) -> Self {
        Self {
            cells: (0..labels.len()).map(|_| Cell::Null).collect(),
            labels,
            bytes: Vec::new(),
        }
    }

    fn read(&mut self, db: &RawDb, statement: *mut sqlite3_stmt) -> Result<()> {
        self.bytes.clear();
        for (index, cell) in self.cells.iter_mut().enumerate() {
            let index = index as c_int;
            *cell = unsafe {
                match sqlite3_column_type(statement, index) {
                    SQLITE_INTEGER => Cell::Integer(sqlite3_column_int64(statement, index)),
                    SQLITE_FLOAT => Cell::Float(sqlite3_column_double(statement, index)),
                    SQLITE_NULL => Cell::Null,
                    kind @ (SQLITE_TEXT | SQLITE_BLOB) => {
                        let data = if kind == SQLITE_TEXT {
                            sqlite3_column_text(statement, index)
                        } else {
                            sqlite3_column_blob(statement, index) as _
                        };
                        let len = sqlite3_column_bytes(statement, index) as usize;
                        let start = self.bytes.len();
                        if data.is_null() {
                            hint::cold_path();
                            // Empty blobs can have a null pointer; text cannot, even when empty.
                            if kind == SQLITE_TEXT
                                || len != 0
                                || sqlite3_errcode(db.as_ptr()) == SQLITE_NOMEM
                            {
                                return Err(db.last_error().context(format!(
                                    "Could not extract the column at index {index}"
                                )));
                            }
                        } else {
                            let raw = slice::from_raw_parts(data, len);
                            if kind == SQLITE_TEXT && str::from_utf8(raw).is_err() {
                                push_lossy(&mut self.bytes, raw);
                                #[cold]
                                #[inline(never)]
                                fn push_lossy(dst: &mut Vec<u8>, raw: &[u8]) {
                                    let mut replacement = [0u8; 4];
                                    let replacement = char::REPLACEMENT_CHARACTER
                                        .encode_utf8(&mut replacement)
                                        .as_bytes();
                                    for chunk in raw.utf8_chunks() {
                                        dst.extend_from_slice(chunk.valid().as_bytes());
                                        if !chunk.invalid().is_empty() {
                                            dst.extend_from_slice(replacement);
                                        }
                                    }
                                }
                            } else {
                                self.bytes.extend_from_slice(raw);
                            }
                        }
                        let range = start..self.bytes.len();
                        if kind == SQLITE_TEXT {
                            Cell::Text(range)
                        } else {
                            Cell::Blob(range)
                        }
                    }
                    kind => {
                        hint::cold_path();
                        return Err(anyhow!(
                            "Unexpected SQLite column type {kind} at index {index}"
                        ));
                    }
                }
            };
        }
        Ok(())
    }
}

/// A stable row snapshot, independent of SQLite's transient column buffers.
#[derive(Debug)]
pub struct SQLiteRow(Arc<SQLiteRowData>);

impl SQLiteRow {
    pub(crate) fn new(labels: RowLabels) -> Self {
        Self(Arc::new(SQLiteRowData::new(labels)))
    }

    #[inline(always)]
    pub(crate) fn read(&mut self, db: &RawDb, statement: *mut sqlite3_stmt) -> Result<Self> {
        match Arc::get_mut(&mut self.0) {
            Some(data) => data.read(db, statement)?,
            None => {
                hint::cold_path();
                // A retained row keeps its snapshot. Otherwise subsequent polls reuse both buffers
                let mut data = SQLiteRowData::new(self.0.labels.clone());
                data.read(db, statement)?;
                self.0 = Arc::new(data);
            }
        }
        Ok(Self(self.0.clone()))
    }
}

impl Row for SQLiteRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.0.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.cells.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        Some(Cow::Owned(match self.0.cells.get(index)? {
            Cell::Null => Value::Null,
            Cell::Integer(value) => Value::Int64(Some(*value)),
            Cell::Float(value) => Value::Float64(Some(*value)),
            Cell::Text(range) => {
                // `SQLiteRowData::read` guarantees every `Cell::Text` range is valid UTF-8.
                let bytes = self.0.bytes.get(range.clone())?;
                Value::Varchar(Some(Cow::Borrowed(unsafe {
                    str::from_utf8_unchecked(bytes)
                })))
            }
            Cell::Blob(range) => {
                Value::Blob(Some(Cow::Borrowed(self.0.bytes.get(range.clone())?)))
            }
        }))
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.0.labels.clone(),
            self.values()
                .map(|value| value.into_owned().into_owned())
                .collect(),
        )
    }
}
