use libsqlite3_sys::*;
use std::{borrow::Cow, ffi::c_int, hint, ops::Range, slice, sync::Arc};
use tank_core::{Error, Result, Row, RowLabels, RowOwned, Value, error_message_from_ptr};

/// A stable row snapshot, independent of SQLite's transient column buffers.
#[derive(Debug)]
pub struct SQLiteRow(Arc<SQLiteRowData>);

#[derive(Debug)]
enum Cell {
    Null,
    Integer(i64),
    Float(f64),
    Text(Range<usize>),
    Blob(Range<usize>),
}

#[derive(Debug)]
pub(crate) struct SQLiteRowData {
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

    #[cold]
    #[inline(never)]
    fn column_error(statement: *mut sqlite3_stmt, index: c_int) -> Error {
        let message =
            unsafe { error_message_from_ptr(sqlite3_errmsg(sqlite3_db_handle(statement))) };
        Error::msg(message.into_owned())
            .context(format!("Could not extract the column at index {index}"))
    }

    fn read(&mut self, statement: *mut sqlite3_stmt) -> Result<()> {
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
                            sqlite3_column_blob(statement, index).cast::<u8>()
                        };
                        let len = sqlite3_column_bytes(statement, index) as usize;
                        let start = self.bytes.len();
                        if data.is_null() {
                            hint::cold_path();
                            // Empty blobs can have a null pointer; text cannot, even when empty.
                            if kind == SQLITE_TEXT
                                || len != 0
                                || sqlite3_errcode(sqlite3_db_handle(statement)) == SQLITE_NOMEM
                            {
                                return Err(Self::column_error(statement, index));
                            }
                        } else {
                            self.bytes
                                .extend_from_slice(slice::from_raw_parts(data, len));
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
                        return Err(Error::msg(format!(
                            "Unexpected SQLite column type {kind} at index {index}"
                        )));
                    }
                }
            };
        }
        Ok(())
    }
}

impl SQLiteRow {
    #[inline(always)]
    pub(crate) fn read(
        storage: &mut Option<Arc<SQLiteRowData>>,
        labels: &RowLabels,
        statement: *mut sqlite3_stmt,
    ) -> Result<Self> {
        let storage = storage.get_or_insert_with(|| Arc::new(SQLiteRowData::new(labels.clone())));
        match Arc::get_mut(storage) {
            Some(data) => data.read(statement)?,
            _ => {
                // A retained row keeps its snapshot. Otherwise subsequent polls reuse
                // both buffers, with no per-cell allocation or shared-label refcounting.
                let mut data = SQLiteRowData::new(labels.clone());
                data.read(statement)?;
                *storage = Arc::new(data);
            }
        }
        Ok(Self(storage.clone()))
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
                Value::Varchar(Some(String::from_utf8_lossy(&self.0.bytes[range.clone()])))
            }
            Cell::Blob(range) => Value::Blob(Some(Cow::Borrowed(&self.0.bytes[range.clone()]))),
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
