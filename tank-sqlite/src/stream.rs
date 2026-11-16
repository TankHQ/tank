use crate::{CBox, SQLiteConnection, SQLiteDriver, SQLitePrepared, SQLiteRow};
use libsqlite3_sys::*;
use std::{
    ffi::{c_char, c_int},
    pin::Pin,
    ptr,
    task::{Context, Poll},
};
use tank_core::{
    Error, Prepared, Query, QueryResult, Result, RowsAffected, stream::Stream, truncate_long,
};

pub(crate) struct SQLiteStream<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    statement: Option<SQLitePrepared>,
    offset: usize,
    generation: usize,
    remaining: u16,
    initialized: bool,
    active: bool,
    done: bool,
}

impl<'s, Q: AsMut<Query<SQLiteDriver>>> SQLiteStream<'s, Q> {
    const BATCH_SIZE: u16 = 256;

    pub(crate) fn new(connection: &'s mut SQLiteConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            statement: None,
            offset: 0,
            generation: 0,
            remaining: Self::BATCH_SIZE,
            initialized: false,
            active: false,
            done: false,
        }
    }

    /// The statement this stream reads from, owned for raw SQL or borrowed from the query.
    #[inline(always)]
    fn prepared(&mut self) -> Option<&mut SQLitePrepared> {
        match self.query.as_mut() {
            Query::Prepared(prepared) => Some(prepared),
            Query::Raw(_) => self.statement.as_mut(),
        }
    }

    #[inline(always)]
    fn statement(&mut self) -> *mut sqlite3_stmt {
        self.prepared()
            .map_or(ptr::null_mut(), |prepared| *prepared.statement)
    }

    #[cold]
    #[inline(never)]
    fn error(&mut self) -> Error {
        let error = Error::msg(SQLiteConnection::last_error(*self.connection.handle))
            .context(format!("While running the query:\n{}", self.query.as_mut()));
        log::error!("{error:#}");
        error
    }

    fn reset(&mut self) {
        if self.active {
            if let Query::Prepared(prepared) = self.query.as_mut() {
                // Step errors are already reported. Raw statements are finalized
                // below; only reusable statements need a separate reset.
                unsafe { sqlite3_reset(*prepared.statement) };
            }
        }
        self.active = false;
        self.initialized = false;
        self.statement = None;
    }

    /// Copies the current row out of the statement.
    #[inline]
    fn row(&mut self) -> Result<Option<SQLiteRow>> {
        if !self.active {
            return Ok(None);
        }
        let statement = self.statement();
        let result = unsafe {
            match sqlite3_step(statement) {
                SQLITE_ROW => {
                    let initialize = !self.initialized;
                    self.initialized = true;
                    self.prepared()
                        .map(|prepared| prepared.read_row(initialize))
                        .transpose()
                }
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
                Query::Prepared(prepared) => {
                    let statement = *prepared.statement;
                    if self.generation != 0 || statement.is_null() {
                        self.done = true;
                        return Ok(None);
                    }
                    if sqlite3_db_handle(statement) != *self.connection.handle {
                        self.done = true;
                        return Err(Error::msg(
                            "Prepared statement belongs to another SQLite connection",
                        ));
                    }
                }
                Query::Raw(raw) => loop {
                    if self.offset == raw.0.len() {
                        self.done = true;
                        return Ok(None);
                    }
                    let sql = raw.0.as_ptr().add(self.offset) as *const c_char;
                    let len = c_int::try_from(raw.0.len() - self.offset)?;
                    let mut statement = SQLitePrepared::new(CBox::new(ptr::null_mut(), |p| {
                        sqlite3_finalize(p);
                    }));
                    let mut tail = ptr::null();
                    let rc = sqlite3_prepare_v2(
                        *self.connection.handle,
                        sql,
                        len,
                        &mut *statement.statement,
                        &mut tail,
                    );
                    if rc != SQLITE_OK {
                        self.done = true;
                        return Err(self.error());
                    }
                    let consumed = tail.offset_from(sql) as usize;
                    if consumed == 0 {
                        self.done = true;
                        return Err(Error::msg(
                            "SQLite made no progress while preparing the query",
                        )
                        .context(format!(
                            "At byte {} of:\n{}",
                            self.offset,
                            truncate_long!(raw.0.get(self.offset..).unwrap_or_default(), true)
                        )));
                    }
                    self.offset += consumed;
                    if !statement.statement.is_null() {
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
            let connection = *self.connection.handle;
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

    #[inline(always)]
    fn next_result(&mut self) -> Option<Result<QueryResult<SQLiteRow>>> {
        loop {
            if self.done {
                return None;
            }
            if self.active {
                match self.row() {
                    Ok(Some(row)) => return Some(Ok(QueryResult::Row(row))),
                    Ok(None) => {}
                    Err(error) => {
                        self.done = true;
                        return Some(Err(error));
                    }
                }
            }
            match self.advance() {
                Ok(Some(affected)) => return Some(Ok(QueryResult::Affected(affected))),
                Ok(None) if self.active => {}
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Stream for SQLiteStream<'_, Q> {
    type Item = Result<QueryResult<SQLiteRow>>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.remaining == 0 {
            this.remaining = Self::BATCH_SIZE;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.remaining -= 1;
        Poll::Ready(this.next_result())
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Drop for SQLiteStream<'_, Q> {
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
        if let Query::Prepared(prepared) = self.query.as_mut()
            && !prepared.statement.is_null()
            && let Err(error) = prepared.clear_bindings()
        {
            log::error!("{error:#}");
        }
    }
}
