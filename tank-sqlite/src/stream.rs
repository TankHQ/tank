use crate::{SQLiteConnection, SQLiteDriver, SQLitePrepared, SQLiteRow};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    hint,
    pin::Pin,
    task::{Context, Poll},
};
use tank_core::{
    Error, Prepared, Query, QueryResult, RawQuery, Result, RowsAffected, stream::Stream,
    truncate_long,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Next,
    Stepping,
    Done,
}

/// Lazily runs one prepared statement or each statement in raw SQL, yielding rows or effects.
pub(crate) struct SQLiteStream<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    statement: Option<SQLitePrepared>,
    offset: usize,
    remaining: u16,
    status: Status,
}

impl<'s, Q: AsMut<Query<SQLiteDriver>>> SQLiteStream<'s, Q> {
    const BATCH_SIZE: u16 = 256;

    #[inline(always)]
    pub(crate) const fn new(connection: &'s mut SQLiteConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            statement: None,
            offset: 0,
            remaining: Self::BATCH_SIZE,
            status: Status::Next,
        }
    }

    #[inline(always)]
    fn active_prepared(&mut self) -> Option<&mut SQLitePrepared> {
        match self.query.as_mut() {
            Query::Prepared(prepared) => Some(prepared),
            Query::Raw(_) => self.statement.as_mut(),
        }
    }

    #[cold]
    #[inline(never)]
    fn step_error(prepared: &SQLitePrepared, handle: *mut sqlite3) -> Error {
        let error =
            SQLiteConnection::last_error(handle).context(format!("While executing {prepared}"));
        // Reset here so a later reset or finalize does not report the same step error again.
        unsafe { sqlite3_reset(*prepared.statement) };
        log::error!("{error:#}");
        error
    }

    #[cold]
    #[inline(never)]
    fn abort(&mut self, error: Error) -> Error {
        self.statement = None;
        self.status = Status::Done;
        error
    }

    /// Steps `prepared` once, returning the next row or `None` at `SQLITE_DONE`.
    #[inline(always)]
    fn step_row(
        prepared: &mut SQLitePrepared,
        handle: *mut sqlite3,
        initialize: bool,
    ) -> Result<Option<SQLiteRow>> {
        match unsafe { sqlite3_step(*prepared.statement) } {
            SQLITE_ROW => prepared.read_row(initialize).map(Some),
            SQLITE_DONE => Ok(None),
            _ => Err(Self::step_error(prepared, handle)),
        }
    }

    /// Steps the active row-returning statement whose column labels are already initialized.
    #[inline(always)]
    fn step_active(&mut self) -> Result<Option<SQLiteRow>> {
        let handle = *self.connection.handle;
        let Some(prepared) = self.active_prepared() else {
            hint::cold_path();
            return Err(anyhow!("Active SQLite stream has no statement"));
        };
        Self::step_row(prepared, handle, false)
    }

    /// Runs a statement without result columns to completion and returns its affected rows.
    fn step_write(prepared: &mut SQLitePrepared, handle: *mut sqlite3) -> Result<RowsAffected> {
        unsafe {
            let statement = *prepared.statement;
            let total_changes_before = sqlite3_total_changes64(handle);
            loop {
                match sqlite3_step(statement) {
                    SQLITE_DONE => break,
                    SQLITE_ROW => {}
                    _ => return Err(Self::step_error(prepared, handle)),
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

    /// Releases the current statement and moves to the next state.
    fn finish_statement(&mut self) {
        match self.query.as_mut() {
            Query::Prepared(prepared) => {
                if !prepared.is_empty() {
                    unsafe { sqlite3_reset(*prepared.statement) };
                }
                self.status = Status::Done;
            }
            Query::Raw(_) => {
                self.statement = None;
                self.status = Status::Next;
            }
        }
    }

    /// Drives an abandoned write to `SQLITE_DONE` so its changes commit.
    fn finish_write(&mut self) {
        let handle = *self.connection.handle;
        let Some(prepared) = self.active_prepared() else {
            return;
        };
        let statement = *prepared.statement;
        if statement.is_null() || unsafe { sqlite3_stmt_readonly(statement) } != 0 {
            return;
        }
        loop {
            match unsafe { sqlite3_step(statement) } {
                SQLITE_ROW => {}
                SQLITE_DONE => return,
                _ => {
                    hint::cold_path();
                    log::error!(
                        "{:#}",
                        SQLiteConnection::last_error(handle)
                            .context("While finishing an abandoned SQLite write")
                    );
                    unsafe { sqlite3_reset(statement) };
                    return;
                }
            }
        }
    }

    /// Prepares the next executable statement, returning `None` when none remain.
    fn prepare_next(&mut self) -> Result<Option<&mut SQLitePrepared>> {
        match self.query.as_mut() {
            Query::Raw(RawQuery(sql)) => loop {
                let rest = sql.get(self.offset..).unwrap_or_default();
                if rest.is_empty() {
                    self.status = Status::Done;
                    return Ok(None);
                }
                let (statement, consumed) = self.connection.prepare_statement::<false>(rest)?;
                if consumed == 0 {
                    hint::cold_path();
                    let error =
                        anyhow!("SQLite made no progress while preparing the query").context(
                            format!("At byte {} of:\n{}", self.offset, truncate_long(sql)),
                        );
                    log::error!("{error:#}");
                    return Err(error);
                }
                self.offset += consumed;
                if !statement.is_empty() {
                    return Ok(Some(self.statement.insert(statement)));
                }
            },
            Query::Prepared(prepared) => {
                if prepared.is_empty() {
                    self.status = Status::Done;
                    return Ok(None);
                }
                if unsafe { sqlite3_db_handle(*prepared.statement) } != *self.connection.handle {
                    hint::cold_path();
                    let error = anyhow!("Prepared statement belongs to another SQLite connection");
                    log::error!("{error:#}");
                    return Err(error);
                }
                Ok(Some(prepared))
            }
        }
    }

    /// Starts the next statement, yielding its affected count or first row.
    #[cold]
    #[inline(never)]
    fn advance(&mut self) -> Result<Option<QueryResult<SQLiteRow>>> {
        let handle = *self.connection.handle;
        loop {
            if self.status == Status::Done {
                return Ok(None);
            }
            let Some(prepared) = self.prepare_next()? else {
                return Ok(None);
            };
            if unsafe { sqlite3_column_count(*prepared.statement) } == 0 {
                let affected = Self::step_write(prepared, handle)?;
                self.finish_statement();
                return Ok(Some(affected.into()));
            }
            if let Some(row) = Self::step_row(prepared, handle, true)? {
                self.status = Status::Stepping;
                return Ok(Some(QueryResult::Row(row)));
            }
            // Statement returned no rows. Release it and continue to the next statement.
            self.finish_statement();
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
        if this.status == Status::Stepping {
            match this.step_active() {
                Ok(Some(row)) => return Poll::Ready(Some(Ok(QueryResult::Row(row)))),
                Ok(None) => this.finish_statement(),
                Err(error) => return Poll::Ready(Some(Err(this.abort(error)))),
            }
        }
        if this.status == Status::Done {
            return Poll::Ready(None);
        }
        match this.advance() {
            Ok(item) => Poll::Ready(item.map(Ok)),
            Err(error) => Poll::Ready(Some(Err(this.abort(error)))),
        }
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Drop for SQLiteStream<'_, Q> {
    fn drop(&mut self) {
        if self.status == Status::Stepping {
            self.finish_write();
            self.finish_statement();
        }
        if let Query::Prepared(prepared) = self.query.as_mut()
            && !prepared.is_empty()
        {
            let _ = prepared.clear_bindings();
        }
    }
}
