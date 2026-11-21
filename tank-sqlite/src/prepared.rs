use crate::{
    CBox, SQLiteConnection, SQLiteRow, ensure_rc, row::SQLiteRowData, sql_writer::SQLiteSqlWriter,
};
use anyhow::anyhow;
use libsqlite3_sys::*;
use rust_decimal::prelude::ToPrimitive;
use std::{
    borrow::Cow,
    ffi::{CStr, c_int},
    fmt::{self, Display},
    hint, mem,
    sync::Arc,
};
use tank_core::{
    AnyQuery, AsValue, Context, Error, ErrorContext, Fragment, Prepared, Result, RowsAffected,
    SqlValueWriter, Value, truncate_long,
};

/// Prepared statement wrapper for SQLite.
#[derive(Debug)]
pub struct SQLitePrepared {
    statement: CBox<*mut sqlite3_stmt>,
    index: u64,
    /// Column labels of the last result, reused when the statement returns the same shape.
    labels: Option<Arc<[String]>>,
    columns: c_int,
    row: Option<Arc<SQLiteRowData>>,
    draft: AnyQuery,
}

impl SQLitePrepared {
    const WRITER: SQLiteSqlWriter = SQLiteSqlWriter {};

    #[inline(always)]
    pub(crate) const fn new(statement: CBox<*mut sqlite3_stmt>) -> Self {
        Self {
            statement,
            index: 0,
            labels: None,
            columns: 0,
            row: None,
            draft: AnyQuery::new(String::new()),
        }
    }

    #[inline(always)]
    pub(crate) fn belongs_to(&self, connection: &SQLiteConnection) -> bool {
        unsafe { sqlite3_db_handle(*self.statement) == *connection.handle }
    }

    #[inline(always)]
    pub(crate) fn has_columns(&self) -> bool {
        unsafe { sqlite3_column_count(*self.statement) > 0 }
    }

    #[inline(always)]
    pub(crate) fn last_insert_rowid(&self) -> i64 {
        unsafe { sqlite3_last_insert_rowid(sqlite3_db_handle(*self.statement)) }
    }

    #[cold]
    #[inline(never)]
    fn step_error(&mut self) -> Error {
        let statement = *self.statement;
        let error = SQLiteConnection::last_error(unsafe { sqlite3_db_handle(statement) })
            .context(format!("While executing {self}"));
        // Reset here so a later reset or finalize does not report the same step error again.
        unsafe { sqlite3_reset(statement) };
        log::error!("{error:#}");
        error
    }

    /// Name of the result column at `index`.
    fn column_name(&self, index: c_int) -> Result<Cow<'_, str>> {
        unsafe {
            let name = sqlite3_column_name(*self.statement, index);
            if !name.is_null() {
                Ok(CStr::from_ptr(name).to_string_lossy())
            } else {
                Err(anyhow!("Missing SQLite column name at index {index}"))
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn init_labels(&mut self) -> Result<()> {
        let columns = unsafe { sqlite3_column_count(*self.statement) };
        let recompiled =
            unsafe { sqlite3_stmt_status(*self.statement, SQLITE_STMTSTATUS_REPREPARE, 1) } != 0;
        if self.labels.is_none() || self.columns != columns || recompiled {
            self.labels = Some(
                (0..columns)
                    .map(|index| self.column_name(index).map(Into::into))
                    .collect::<Result<_>>()?,
            );
            self.columns = columns;
            self.row = None;
        }
        Ok(())
    }

    #[inline(always)]
    fn read_row(&mut self, initialize: bool) -> Result<SQLiteRow> {
        if initialize {
            self.init_labels()?;
        }
        let statement = *self.statement;
        let Some(labels) = self.labels.as_ref() else {
            return Err(Error::msg(
                "Row labels were not initialized before reading a row",
            ));
        };
        SQLiteRow::read(&mut self.row, labels, statement)
    }

    /// Steps once, returning the next row or `None` at `SQLITE_DONE`.
    #[inline(always)]
    pub(crate) fn step_row(&mut self, initialize: bool) -> Result<Option<SQLiteRow>> {
        match unsafe { sqlite3_step(*self.statement) } {
            SQLITE_ROW => self.read_row(initialize).map(Some),
            SQLITE_DONE => Ok(None),
            _ => Err(self.step_error()),
        }
    }

    /// Steps once and expects immediate completion (`SQLITE_DONE`).
    #[inline(always)]
    pub(crate) fn step_done(&mut self) -> Result<()> {
        if unsafe { sqlite3_step(*self.statement) } != SQLITE_DONE {
            hint::cold_path();
            return Err(self.step_error());
        }
        Ok(())
    }

    /// Runs a statement without result columns to completion and returns its affected rows.
    pub(crate) fn step_write(&mut self) -> Result<RowsAffected> {
        unsafe {
            let statement = *self.statement;
            let handle = sqlite3_db_handle(statement);
            let total_changes_before = sqlite3_total_changes64(handle);
            loop {
                match sqlite3_step(statement) {
                    SQLITE_DONE => break,
                    SQLITE_ROW => {}
                    _ => return Err(self.step_error()),
                }
            }
            // `changes64` is stale after DDL. `total_changes64` detects DML while `changes64` excludes triggers.
            let rows_affected = if sqlite3_total_changes64(handle) != total_changes_before {
                sqlite3_changes64(handle) as u64
            } else {
                0
            };
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: Some(sqlite3_last_insert_rowid(handle)),
            })
        }
    }

    /// Completes any started write so its changes commit, then resets the statement.
    pub(crate) fn finish(&mut self) {
        let statement = *self.statement;
        if statement.is_null() {
            return;
        }
        unsafe {
            // Drive an abandoned write to `SQLITE_DONE` so its changes commit.
            // Reads are never walked because they can be unbounded.
            if sqlite3_stmt_busy(statement) != 0 && sqlite3_stmt_readonly(statement) == 0 {
                loop {
                    match sqlite3_step(statement) {
                        SQLITE_ROW => {}
                        SQLITE_DONE => break,
                        _ => {
                            hint::cold_path();
                            log::error!(
                                "{:#}",
                                SQLiteConnection::last_error(sqlite3_db_handle(statement))
                                    .context("While finishing an abandoned SQLite write")
                            );
                            break;
                        }
                    }
                }
            }
            // Release locks and clear any prior step error so later reset or finalize succeeds.
            sqlite3_reset(statement);
        }
    }

    /// Binds a temporal value written by the dialect, whose text only lives for this call.
    #[inline]
    unsafe fn bind_written(
        statement: *mut sqlite3_stmt,
        index: c_int,
        buffer: &mut AnyQuery,
        write: impl Fn(&mut Context, &mut AnyQuery),
    ) -> c_int {
        buffer.clear();
        write(&mut Context::fragment(Fragment::ParameterBinding), buffer);
        let v = buffer.as_str();
        unsafe {
            sqlite3_bind_text(
                statement,
                index,
                v.as_ptr() as _,
                v.len() as _,
                SQLITE_TRANSIENT(),
            )
        }
    }

    #[cold]
    #[inline(never)]
    fn unsupported_value(index: c_int, value: Value<'_>) -> Error {
        anyhow!("Cannot use {value:?} as the parameter at index {index}")
    }
}

impl Prepared for SQLitePrepared {
    #[inline(always)]
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.statement.is_null()
    }

    #[inline(always)]
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        if self.is_empty() {
            self.index = 0;
            return Ok(self);
        }
        unsafe {
            ensure_rc!(
                sqlite3_reset(*self.statement),
                SQLiteConnection::last_error(sqlite3_db_handle(*self.statement)),
                "While resetting SQLite statement"
            );
            ensure_rc!(
                sqlite3_clear_bindings(*self.statement),
                SQLiteConnection::last_error(sqlite3_db_handle(*self.statement)),
                "While clearing SQLite statement bindings"
            );
        }
        self.index = 0;
        Ok(self)
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)?;
        Ok(self)
    }

    #[inline(always)]
    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        // SQLite's parameter index is 1-based.
        let index = index as c_int + 1;
        let value = v.as_value();
        let statement = *self.statement;
        unsafe {
            let rc = if value.is_null() {
                sqlite3_bind_null(statement, index)
            } else {
                'simple_value: {
                    let rc = match value {
                        Value::Boolean(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int8(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int16(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int32(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int64(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::Int128(Some(ref v)) => {
                            break 'simple_value match sqlite3_int64::try_from(**v) {
                                Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                                Err(_) => {
                                    hint::cold_path();
                                    return Err(Self::unsupported_value(index, value));
                                }
                            };
                        }
                        Value::UInt8(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt16(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt32(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt64(Some(v)) => match sqlite3_int64::try_from(v) {
                            Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                            Err(_) => {
                                hint::cold_path();
                                return Err(Self::unsupported_value(index, value));
                            }
                        },
                        Value::UInt128(Some(ref v)) => {
                            break 'simple_value match sqlite3_int64::try_from(**v) {
                                Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                                Err(_) => {
                                    hint::cold_path();
                                    return Err(Self::unsupported_value(index, value));
                                }
                            };
                        }
                        Value::Float32(Some(v)) => sqlite3_bind_double(statement, index, v as _),
                        Value::Float64(Some(v)) => sqlite3_bind_double(statement, index, v as _),
                        Value::Decimal(Some(v), ..) => match v.to_f64() {
                            Some(v) => sqlite3_bind_double(statement, index, v as _),
                            None => {
                                hint::cold_path();
                                return Err(Self::unsupported_value(index, value));
                            }
                        },
                        Value::Char(Some(v)) => {
                            let mut buffer = [0u8; 4];
                            let value = v.encode_utf8(&mut buffer);
                            sqlite3_bind_text(
                                statement,
                                index,
                                value.as_ptr() as _,
                                value.len() as _,
                                SQLITE_TRANSIENT(),
                            )
                        }
                        Value::Varchar(Some(ref v)) => {
                            let rc = sqlite3_bind_text(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                SQLITE_TRANSIENT(),
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Blob(Some(ref v)) => {
                            let rc = sqlite3_bind_blob(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                SQLITE_TRANSIENT(),
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Date(Some(v)) => {
                            Self::bind_written(statement, index, &mut self.draft, |context, out| {
                                Self::WRITER.write_date(context, out, &v)
                            })
                        }
                        Value::Time(Some(v)) => {
                            Self::bind_written(statement, index, &mut self.draft, |context, out| {
                                Self::WRITER.write_time(context, out, &v)
                            })
                        }
                        Value::Timestamp(Some(v)) => {
                            Self::bind_written(statement, index, &mut self.draft, |context, out| {
                                Self::WRITER.write_timestamp(context, out, &v)
                            })
                        }
                        Value::TimestampWithTimezone(Some(v)) => {
                            Self::bind_written(statement, index, &mut self.draft, |context, out| {
                                Self::WRITER.write_timestamptz(context, out, &v)
                            })
                        }
                        Value::Uuid(Some(v)) => {
                            let mut buffer = [0u8; 36];
                            let v = v.hyphenated().encode_lower(&mut buffer);
                            sqlite3_bind_text(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                SQLITE_TRANSIENT(),
                            )
                        }
                        _ => {
                            hint::cold_path();
                            return Err(Self::unsupported_value(index, value));
                        }
                    };
                    // Avoid an opaque Value::drop call for simple values that don't allocate memory.
                    mem::forget(value);
                    rc
                }
            };
            ensure_rc!(
                rc,
                SQLiteConnection::last_error(sqlite3_db_handle(*self.statement)),
                format!("While binding {} (0-based) to {self}", index - 1),
            );
        }
        self.index = index as u64;
        Ok(self)
    }
}

impl Drop for SQLitePrepared {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Display for SQLitePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            f.write_str("SQLitePrepared:")?;
            let ptr = sqlite3_sql(*self.statement);
            if !ptr.is_null() {
                f.write_str("\n")?;
                write!(
                    f,
                    "{}",
                    truncate_long(&CStr::from_ptr(ptr).to_string_lossy())
                )?;
            } else {
                write!(f, " {:p}", *self.statement)?;
            }
        }
        Ok(())
    }
}
