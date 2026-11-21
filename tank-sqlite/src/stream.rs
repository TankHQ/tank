use crate::{SQLiteConnection, SQLiteDriver, SQLitePrepared, SQLiteRow};
use libsqlite3_sys::*;
use std::{
    hint,
    pin::Pin,
    ptr,
    task::{Context, Poll},
};
use tank_core::{
    Error, Prepared, Query, QueryResult, RawQuery, Result, RowsAffected, stream::Stream,
    truncate_long,
};

pub(crate) struct SQLiteStream<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    statement: Option<SQLitePrepared>,
    offset: usize,
    stream_started: bool,
    remaining: u16,
    is_initialized: bool,
    active: bool,
    done: bool,
}

impl<'s, Q: AsMut<Query<SQLiteDriver>>> SQLiteStream<'s, Q> {
    const BATCH_SIZE: u16 = 256;

    pub(crate) const fn new(connection: &'s mut SQLiteConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            statement: None,
            offset: 0,
            stream_started: false,
            remaining: Self::BATCH_SIZE,
            is_initialized: false,
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

    fn reset(&mut self) {
        if self.active {
            if let Query::Prepared(prepared) = self.query.as_mut() {
                // Step errors are already reported. Raw statements are finalized
                // below; only reusable statements need a separate reset.
                unsafe { sqlite3_reset(*prepared.statement) };
            }
        }
        self.active = false;
        self.is_initialized = false;
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
                    let initialize = !self.is_initialized;
                    self.is_initialized = true;
                    self.prepared()
                        .map(|prepared| prepared.read_row(initialize))
                        .transpose()
                }
                SQLITE_DONE => Ok(None),
                _ => Err(SQLiteConnection::last_error(*self.connection.handle)),
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
                Query::Raw(RawQuery(sql)) => loop {
                    if self.offset >= sql.len() {
                        self.done = true;
                        return Ok(None);
                    }
                    let rest = sql.get(self.offset..).unwrap_or_default();
                    let (statement, consumed) = self.connection.prepare_statement::<false>(rest)?;
                    if consumed == 0 {
                        hint::cold_path();
                        self.done = true;
                        return Err(Error::msg(
                            "SQLite made no progress while preparing the query",
                        )
                        .context(format!(
                            "At byte {} of:\n{}",
                            self.offset,
                            truncate_long(sql)
                        )));
                    }
                    self.offset += consumed;
                    if !statement.statement.is_null() {
                        self.statement = Some(statement);
                        break;
                    }
                },
                Query::Prepared(prepared) => {
                    let statement = *prepared.statement;
                    if self.stream_started || statement.is_null() {
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
            }
            // Set before stepping so a finished prepared statement is not started again.
            self.stream_started = true;
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
