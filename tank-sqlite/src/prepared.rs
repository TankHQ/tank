use crate::{RawDb, SQLiteConnection, SQLiteRow, ensure_rc, sql_writer::SQLiteSqlWriter};
use anyhow::anyhow;
use libsqlite3_sys::*;
use rust_decimal::prelude::ToPrimitive;
use std::{
    borrow::Cow,
    ffi::{CStr, c_int},
    fmt::{self, Display, Write as _},
    hint, mem, ptr,
    sync::{Arc, Mutex, PoisonError},
};
use tank_core::{
    AnyQuery, AsEntity, AsValue, Context, Entity, Error, ErrorContext, Fragment, Prepared,
    QueryResult, Result, Row, RowsAffected, SqlValueWriter, Value, truncate_long,
};

/// Prepared statement wrapper for SQLite.
#[derive(Debug)]
pub struct SQLitePrepared {
    db: Arc<Mutex<RawDb>>,
    statement: *mut sqlite3_stmt,
    index: u64,
    /// Reusable row snapshot storage and column labels for the current result shape.
    row: Option<SQLiteRow>,
    draft: AnyQuery,
}

impl SQLitePrepared {
    const WRITER: SQLiteSqlWriter = SQLiteSqlWriter {};

    #[inline(always)]
    pub(crate) const fn new(db: Arc<Mutex<RawDb>>, statement: *mut sqlite3_stmt) -> Self {
        Self {
            db,
            statement,
            index: 0,
            row: None,
            draft: AnyQuery::new(String::new()),
        }
    }

    #[inline(always)]
    pub(crate) fn belongs_to(&self, connection: &SQLiteConnection) -> bool {
        Arc::ptr_eq(&self.db, &connection.db)
    }

    #[cold]
    #[inline(never)]
    fn step_error(&self, db: &RawDb) -> Error {
        let error = db.last_error().context(format!("While executing {self}"));
        // Reset here so a later reset or finalize does not report the same step error again.
        unsafe { sqlite3_reset(self.statement) };
        log::error!("{error:#}");
        error
    }

    #[cold]
    #[inline(never)]
    fn init_row(statement: *mut sqlite3_stmt, row: &mut Option<SQLiteRow>) -> Result<()> {
        let columns = unsafe { sqlite3_column_count(statement) };
        let recompiled =
            unsafe { sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_REPREPARE, 1) } != 0;
        if row.as_ref().is_none_or(|row| row.len() != columns as usize) || recompiled {
            let labels = (0..columns)
                .map(|index| unsafe {
                    let name = sqlite3_column_name(statement, index);
                    if !name.is_null() {
                        Ok(CStr::from_ptr(name).to_string_lossy().into_owned())
                    } else {
                        Err(anyhow!("Missing SQLite column name at index {index}"))
                    }
                })
                .collect::<Result<_>>()?;
            *row = Some(SQLiteRow::new(labels));
        }
        Ok(())
    }

    /// Steps a row-returning statement, returning `Err(None)` on a SQLite step failure.
    #[inline(always)]
    fn do_step_row_locked(
        db: &RawDb,
        statement: *mut sqlite3_stmt,
        row: &mut Option<SQLiteRow>,
        initialize: bool,
    ) -> std::result::Result<Option<SQLiteRow>, Option<Error>> {
        match unsafe { sqlite3_step(statement) } {
            SQLITE_ROW => {
                if initialize {
                    Self::init_row(statement, row).map_err(Some)?;
                }
                let Some(row) = row.as_mut() else {
                    hint::cold_path();
                    return Err(Some(anyhow!(
                        "Row storage was not initialized before reading a row"
                    )));
                };
                row.read(db, statement).map(Some).map_err(Some)
            }
            SQLITE_DONE => Ok(None),
            _ => Err(None),
        }
    }

    /// Steps once, returning the next row or `None` at `SQLITE_DONE`.
    #[inline(always)]
    pub(crate) fn do_step_row(&mut self, initialize: bool) -> Result<Option<SQLiteRow>> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        Self::do_step_row_locked(&db, self.statement, &mut self.row, initialize)
            .map_err(|error| error.unwrap_or_else(|| self.step_error(&db)))
    }

    /// Binds and steps a batch of entities under a single DB lock, returning affected rows.
    ///
    /// Borrowed strings and blobs bind with `SQLITE_STATIC` because each entity
    /// lives across `bind`, `step`, and `reset`, and `self` is never exposed to callers.
    pub(crate) fn append_batch<E: AsEntity>(
        &mut self,
        batch: impl IntoIterator<Item = E>,
    ) -> Result<RowsAffected> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        let statement = self.statement;
        let mut count = 0;
        for entity in batch {
            for value in entity.as_entity().row_values() {
                let index = self.index;
                ensure_rc!(
                    Self::bind_value::<true>(
                        statement,
                        &mut self.draft,
                        &mut self.index,
                        value,
                        index,
                    )?,
                    db.last_error(),
                    format!("While binding {index} (0-based) to {self}"),
                );
            }
            unsafe {
                if sqlite3_step(statement) != SQLITE_DONE {
                    hint::cold_path();
                    return Err(self.step_error(&db));
                }
                sqlite3_reset(statement);
            }
            self.index = 0;
            count += 1;
        }
        Ok(RowsAffected {
            rows_affected: Some(count),
            last_affected_id: Some(unsafe { sqlite3_last_insert_rowid(db.as_ptr()) }),
        })
    }

    /// Runs a statement's first step for `fetch`, returning its first row or draining if columnless.
    pub(crate) fn start_row(&mut self) -> Result<Option<SQLiteRow>> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        let statement = self.statement;
        if unsafe { sqlite3_column_count(statement) } > 0 {
            return Self::do_step_row_locked(&db, statement, &mut self.row, true)
                .map_err(|error| error.unwrap_or_else(|| self.step_error(&db)));
        }
        unsafe {
            loop {
                match sqlite3_step(statement) {
                    SQLITE_DONE => return Ok(None),
                    SQLITE_ROW => {}
                    _ => return Err(self.step_error(&db)),
                }
            }
        }
    }

    /// Runs a statement's first step, returning its first row or affected rows if columnless.
    pub(crate) fn start(&mut self) -> Result<Option<QueryResult<SQLiteRow>>> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        let statement = self.statement;
        if unsafe { sqlite3_column_count(statement) } > 0 {
            return Self::do_step_row_locked(&db, statement, &mut self.row, true)
                .map(|row| row.map(QueryResult::Row))
                .map_err(|error| error.unwrap_or_else(|| self.step_error(&db)));
        }
        unsafe {
            let handle = db.as_ptr();
            let total_changes_before = sqlite3_total_changes64(handle);
            let last_rowid_before = sqlite3_last_insert_rowid(handle);
            loop {
                match sqlite3_step(statement) {
                    SQLITE_DONE => break,
                    SQLITE_ROW => {}
                    _ => return Err(self.step_error(&db)),
                }
            }
            // `changes64` is stale after DDL. `total_changes64` detects DML while `changes64` excludes triggers.
            let rows_affected = if sqlite3_total_changes64(handle) != total_changes_before {
                sqlite3_changes64(handle) as u64
            } else {
                0
            };
            let last_rowid_after = sqlite3_last_insert_rowid(handle);
            let last_affected_id = (rows_affected > 0 && last_rowid_after != last_rowid_before)
                .then_some(last_rowid_after);
            Ok(Some(
                RowsAffected {
                    rows_affected: Some(rows_affected),
                    last_affected_id,
                }
                .into(),
            ))
        }
    }

    /// Completes any started write so its changes commit, then resets the statement.
    fn finish_locked(db: &RawDb, statement: *mut sqlite3_stmt) {
        if statement.is_null() {
            return;
        }
        unsafe {
            // Drive an abandoned write to `SQLITE_DONE` so its changes commit.
            // Reads are never walked because they can be unbounded.
            if sqlite3_stmt_busy(statement) != 0 && sqlite3_stmt_readonly(statement) == 0 {
                hint::cold_path();
                loop {
                    match sqlite3_step(statement) {
                        SQLITE_ROW => {}
                        SQLITE_DONE => break,
                        _ => {
                            log::error!(
                                "{:#}",
                                db.last_error()
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

    #[inline(always)]
    fn clear_bindings_inner(db: &RawDb, statement: *mut sqlite3_stmt) -> Result<()> {
        Self::finish_locked(db, statement);
        unsafe {
            ensure_rc!(
                sqlite3_clear_bindings(statement),
                db.last_error(),
                "While clearing SQLite statement bindings"
            );
        }
        Ok(())
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
            sqlite3_bind_text64(
                statement,
                index,
                v.as_ptr() as _,
                v.len() as _,
                SQLITE_TRANSIENT(),
                SQLITE_UTF8 as _,
            )
        }
    }

    #[cold]
    #[inline(never)]
    fn unsupported_value(index: c_int, value: Value<'_>) -> Error {
        anyhow!("Cannot use {value:?} as the parameter at index {index}")
    }

    #[inline(always)]
    fn bind_value<const STATIC_BORROW: bool>(
        statement: *mut sqlite3_stmt,
        draft: &mut AnyQuery,
        next_index: &mut u64,
        v: impl AsValue,
        index: u64,
    ) -> Result<c_int> {
        // SQLite's parameter index is 1-based.
        let index = c_int::try_from(index)
            .ok()
            .and_then(|i| i.checked_add(1))
            .ok_or_else(|| anyhow!("SQLite parameter index {index} exceeds maximum"))?;
        let value = v.as_value();
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
                            let destructor = if STATIC_BORROW && matches!(v, Cow::Borrowed(_)) {
                                SQLITE_STATIC()
                            } else {
                                SQLITE_TRANSIENT()
                            };
                            let rc = sqlite3_bind_text64(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                destructor,
                                SQLITE_UTF8 as _,
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Blob(Some(ref v)) => {
                            let destructor = if STATIC_BORROW && matches!(v, Cow::Borrowed(_)) {
                                SQLITE_STATIC()
                            } else {
                                SQLITE_TRANSIENT()
                            };
                            let rc = sqlite3_bind_blob64(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                destructor,
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Date(Some(v)) => {
                            Self::bind_written(statement, index, draft, |context, out| {
                                Self::WRITER.write_date(context, out, &v)
                            })
                        }
                        Value::Time(Some(v)) => {
                            Self::bind_written(statement, index, draft, |context, out| {
                                Self::WRITER.write_time(context, out, &v)
                            })
                        }
                        Value::Timestamp(Some(v)) => {
                            Self::bind_written(statement, index, draft, |context, out| {
                                Self::WRITER.write_timestamp(context, out, &v)
                            })
                        }
                        Value::TimestampWithTimezone(Some(v)) => {
                            Self::bind_written(statement, index, draft, |context, out| {
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
                        Value::IpAddr(Some(v)) => {
                            Self::bind_written(statement, index, draft, |_, out| {
                                let _ = write!(out, "{v}");
                            })
                        }
                        Value::Json(Some(ref v)) => {
                            break 'simple_value Self::bind_written(
                                statement,
                                index,
                                draft,
                                |_, out| {
                                    let _ = write!(out, "{v}");
                                },
                            );
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
            if rc == SQLITE_OK {
                *next_index = index as u64;
            }
            Ok(rc)
        }
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
        if !self.is_empty() {
            let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
            Self::clear_bindings_inner(&db, self.statement)?;
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
        {
            let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
            ensure_rc!(
                Self::bind_value::<false>(
                    self.statement,
                    &mut self.draft,
                    &mut self.index,
                    v,
                    index,
                )?,
                db.last_error(),
                format!("While binding {index} (0-based) to {self}"),
            );
        }
        Ok(self)
    }
}

// Every SQLite C API call on `statement` locks `db`, except `sqlite3_sql` which reads an immutable
// string owned by `statement` for its lifetime.
unsafe impl Send for SQLitePrepared {}
unsafe impl Sync for SQLitePrepared {}

impl Display for SQLitePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            f.write_str("SQLitePrepared:")?;
            if !self.statement.is_null() {
                let ptr = sqlite3_sql(self.statement);
                if !ptr.is_null() {
                    f.write_str("\n")?;
                    return write!(
                        f,
                        "{}",
                        truncate_long(&CStr::from_ptr(ptr).to_string_lossy())
                    );
                }
            }
            write!(f, " {:p}", self.statement)
        }
    }
}

impl Drop for SQLitePrepared {
    fn drop(&mut self) {
        if !self.statement.is_null() {
            let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
            let statement = mem::replace(&mut self.statement, ptr::null_mut());
            Self::finish_locked(&db, statement);
            db.finalize(statement);
        }
    }
}
