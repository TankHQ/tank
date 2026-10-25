use crate::{
    CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction,
    extract::{extract_name, extract_value},
};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_int},
    ptr,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Prepared, Query, QueryResult, RawQuery,
    Result, RowOwned, RowsAffected, describe_path, error_message_from_ptr, truncate_long,
};

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
///
/// Provides helpers to prepare/execute statements and stream results into `tank_core` result types.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) connection: CBox<*mut sqlite3>,
}

impl SQLiteConnection {
    pub fn last_error(&self) -> String {
        unsafe {
            let errcode = sqlite3_errcode(*self.connection);
            format!(
                "Error ({errcode}): {}",
                error_message_from_ptr(&sqlite3_errmsg(*self.connection)),
            )
        }
    }
}

// Both iterator levels own this state. Even a rowset retained after the outer iterator
// is dropped keeps the connection and prepared query borrowed. All native access is
// serialized by the mutex; no worker can outlive those borrows.
struct SQLiteCursor<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    statement: Option<SQLitePrepared>,
    offset: usize,
    generation: usize,
    active: bool,
    done: bool,
    names: Option<Arc<[String]>>,
}

impl<Q: AsMut<Query<SQLiteDriver>>> SQLiteCursor<'_, Q> {
    fn statement(&mut self) -> *mut sqlite3_stmt {
        match self.query.as_mut() {
            Query::Prepared(prepared) => prepared.statement(),
            Query::Raw(_) => self
                .statement
                .as_ref()
                .map_or(ptr::null_mut(), |s| s.statement()),
        }
    }

    fn error(&mut self) -> Error {
        Error::msg(self.connection.last_error())
            .context(format!("While running the query:\n{}", self.query.as_mut()))
    }

    fn reset(&mut self) {
        let statement = self.statement();
        if !statement.is_null() {
            // A step error is already returned to the caller. Reset reports that same
            // error again, but still resets the VM, so do not report it twice.
            unsafe { sqlite3_reset(statement) };
        }
        self.active = false;
        self.names = None;
        self.statement = None;
    }

    fn row(&mut self) -> Result<Option<RowOwned>> {
        if !self.active {
            return Ok(None);
        }
        let statement = self.statement();
        let result = unsafe {
            match sqlite3_step(statement) {
                SQLITE_ROW => (|| {
                    let count = sqlite3_column_count(statement);
                    // Read labels after stepping: SQLite may have recompiled the VM.
                    let names = match &self.names {
                        Some(names) => names.clone(),
                        None => {
                            let names = (0..count)
                                .map(|i| extract_name(statement, i))
                                .collect::<Result<Arc<[_]>>>()?;
                            self.names = Some(names.clone());
                            names
                        }
                    };
                    let values = (0..count)
                        .map(|i| extract_value(statement, i))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Some(RowOwned { names, values }))
                })(),
                SQLITE_DONE => Ok(None),
                _ => Err(self.error()),
            }
        };
        if !matches!(&result, Ok(Some(_))) {
            if result.is_err() {
                self.done = true;
            }
            self.reset();
        }
        result
    }

    fn advance(&mut self) -> Result<Option<RowsAffected>> {
        // Discard one row at a time, including extraction errors, before moving on.
        while self.row()?.is_some() {}
        if self.done {
            return Ok(None);
        }
        unsafe {
            match self.query.as_mut() {
                Query::Prepared(_) => {
                    if self.generation != 0 || self.statement().is_null() {
                        self.done = true;
                        return Ok(None);
                    }
                }
                Query::Raw(raw) => loop {
                    if self.offset == raw.sql.len() {
                        self.done = true;
                        return Ok(None);
                    }
                    let sql = raw.sql.as_ptr().add(self.offset) as *const c_char;
                    let len = c_int::try_from(raw.sql.len() - self.offset)?;
                    let mut statement = SQLitePrepared::new(CBox::new(ptr::null_mut(), |p| {
                        sqlite3_finalize(p);
                    }));
                    let mut tail = ptr::null();
                    let rc = sqlite3_prepare_v2(
                        *self.connection.connection,
                        sql,
                        len,
                        &mut *statement.statement,
                        &mut tail,
                    );
                    if rc != SQLITE_OK {
                        self.done = true;
                        return Err(self.error());
                    }
                    self.offset += tail.offset_from(sql) as usize;
                    if !statement.statement().is_null() {
                        self.statement = Some(statement);
                        break;
                    }
                },
            }
            self.generation += 1;
            self.active = true;
            let statement = self.statement();
            if sqlite3_column_count(statement) > 0 {
                return Ok(None);
            }
            let connection = *self.connection.connection;
            let before = sqlite3_total_changes64(connection);
            while self.row()?.is_some() {}
            // changes64 is stale after DDL; total_changes distinguishes it from DML.
            // Use changes64 rather than the delta to exclude trigger side effects.
            Ok(Some(RowsAffected {
                rows_affected: Some(if sqlite3_total_changes64(connection) != before {
                    sqlite3_changes64(connection) as u64
                } else {
                    0
                }),
                last_affected_id: Some(sqlite3_last_insert_rowid(connection)),
            }))
        }
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Drop for SQLiteCursor<'_, Q> {
    fn drop(&mut self) {
        let statement = self.statement();
        if self.active && !statement.is_null() && unsafe { sqlite3_stmt_readonly(statement) == 0 } {
            // Finish an active RETURNING write, but never walk an abandoned SELECT.
            loop {
                match self.row() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        // Drop cannot return errors; normal row/outer advancement can.
                        log::error!("While finishing an abandoned SQLite write: {error:#}");
                        break;
                    }
                }
            }
        }
        self.reset();
        if let Query::Prepared(prepared) = self.query.as_mut() {
            if !prepared.statement().is_null() {
                if let Err(error) = prepared.clear_bindings() {
                    log::error!("{error:#}");
                }
            }
        }
    }
}

impl Connection for SQLiteConnection {
    async fn connect(driver: &SQLiteDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to SQLite {}",
                describe_path::<SQLiteDriver>(&url)
            )
        };
        let connection_string = CString::from_str(&url.as_str().replacen("sqlite://", "file:", 1))
            .with_context(make_context)?;
        let mut connection;
        unsafe {
            connection = CBox::new(ptr::null_mut(), |p| {
                if sqlite3_close(p) != SQLITE_OK {
                    let error = Error::msg(error_message_from_ptr(&sqlite3_errmsg(p)).to_string())
                        .context("While closing the sqlite connection");
                    log::error!("{error:#}");
                }
            });
            let rc = sqlite3_open_v2(
                connection_string.as_ptr(),
                &mut *connection,
                SQLITE_OPEN_READWRITE
                    | SQLITE_OPEN_CREATE
                    | SQLITE_OPEN_URI
                    | SQLITE_OPEN_FULLMUTEX,
                ptr::null(),
            );
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(&sqlite3_errmsg(*connection)).to_string())
                        .context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            let rc = sqlite3_exec(
                *connection,
                c"PRAGMA foreign_keys = ON".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(&sqlite3_errmsg(*connection)).to_string())
                        .context("While enabling foreign key enforcement");
                log::error!("{error:#}");
            }
        }
        Ok(Self { connection })
    }

    fn begin(&mut self) -> impl Future<Output = Result<SQLiteTransaction<'_>>> + Send {
        SQLiteTransaction::new(self)
    }
}

impl Executor for SQLiteConnection {
    type Driver = SQLiteDriver;

    async fn do_prepare(&mut self, RawQuery { sql, .. }: RawQuery) -> Result<Query<SQLiteDriver>> {
        let prepared = unsafe {
            let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
            let connection = *self.connection;
            let len = c_int::try_from(sql.len())?;
            let sql = CString::new(sql.as_bytes())?;
            let mut statement = CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                if p.is_null() {
                    return;
                }
                let db = sqlite3_db_handle(p);
                let rc = sqlite3_finalize(p);
                if rc != SQLITE_OK {
                    let error = Error::msg(error_message_from_ptr(&sqlite3_errmsg(db)).to_string())
                        .context("While finalizing a prepared statement");
                    log::error!("{error:#}");
                }
            });
            let mut tail = ptr::null();
            let rc = sqlite3_prepare_v2(connection, sql.as_ptr(), len, &mut *statement, &mut tail);
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(&sqlite3_errmsg(connection)).to_string())
                        .context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            if tail != ptr::null() && *tail != '\0' as i8 {
                let error = anyhow!(
                    "Cannot prepare more than one statement at a time (remaining: {})",
                    CStr::from_ptr(tail).to_str().unwrap_or("unprintable")
                )
                .context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            statement
        };
        Ok(SQLitePrepared::new(prepared).into())
    }

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as tank_core::Driver>::Results<'s, Q::Output>>>
    + Send
    where
        Q: AsQuery<SQLiteDriver> + Send + 's,
    {
        async move {
            let mut query = query.as_query();
            if let Query::Raw(raw) = query.as_mut() {
                if raw.sql.as_bytes().contains(&0) {
                    return Err(anyhow!("SQLite query contains an interior NUL byte"));
                }
            }
            if let Query::Prepared(prepared) = query.as_mut() {
                if !prepared.statement().is_null()
                    && unsafe { sqlite3_db_handle(prepared.statement()) } != *self.connection
                {
                    return Err(anyhow!(
                        "Prepared statement belongs to another SQLite connection"
                    ));
                }
            }
            let state = Arc::new(Mutex::new(SQLiteCursor {
                connection: self,
                query,
                statement: None,
                offset: 0,
                generation: 0,
                active: false,
                done: false,
                names: None,
            }));
            Ok(SQLiteResults { state, done: false })
        }
    }
}

/// Iterator over the rows of one SQLite statement.
///
/// Shares state with [`SQLiteResults`]; a generation counter invalidates it once the next statement begins.
pub struct SQLiteRows<'s, Q: AsMut<Query<SQLiteDriver>>> {
    state: Arc<Mutex<SQLiteCursor<'s, Q>>>,
    generation: usize,
}

impl<Q: AsMut<Query<SQLiteDriver>>> Iterator for SQLiteRows<'_, Q> {
    type Item = Result<RowOwned>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut cursor = self.state.lock().unwrap();
        if cursor.generation != self.generation {
            return None;
        }
        cursor.row().transpose()
    }
}

/// Statement-level results of a SQLite query.
pub struct SQLiteResults<'s, Q: AsMut<Query<SQLiteDriver>>> {
    state: Arc<Mutex<SQLiteCursor<'s, Q>>>,
    done: bool,
}

impl<'s, Q: AsMut<Query<SQLiteDriver>>> Iterator for SQLiteResults<'s, Q> {
    type Item = Result<QueryResult<SQLiteRows<'s, Q>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut cursor = self.state.lock().unwrap();
        match cursor.advance() {
            Err(error) => {
                cursor.done = true;
                self.done = true;
                Some(Err(error))
            }
            Ok(Some(affected)) => Some(Ok(QueryResult::Affected(affected))),
            Ok(None) if cursor.active => {
                let generation = cursor.generation;
                Some(Ok(QueryResult::Rows(SQLiteRows {
                    state: self.state.clone(),
                    generation,
                })))
            }
            Ok(None) => {
                self.done = true;
                None
            }
        }
    }
}
