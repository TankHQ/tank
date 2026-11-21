use crate::{SQLiteConnection, SQLiteDriver, SQLitePrepared, SQLiteRow};
use anyhow::anyhow;
use std::{
    hint,
    pin::Pin,
    task::{Context, Poll},
};
use tank_core::{
    Error, Prepared, Query, QueryResult, RawQuery, Result, stream::Stream, truncate_long,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Next,     // Advance in the SQL and prepare another statement
    Stepping, // Do step on the current prepared statemenet
    Done,     // The stream is done
}

/// Lazily runs one prepared statement or each statement in raw SQL, yielding rows or effects.
pub(crate) struct SQLiteStream<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    current_statement: Option<SQLitePrepared>,
    offset: usize,
    remaining: u16,
    state: State,
}

impl<'s, Q: AsMut<Query<SQLiteDriver>>> SQLiteStream<'s, Q> {
    const BATCH_SIZE: u16 = 256;

    #[inline(always)]
    pub(crate) const fn new(connection: &'s mut SQLiteConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            current_statement: None,
            offset: 0,
            remaining: Self::BATCH_SIZE,
            state: State::Next,
        }
    }

    #[cold]
    #[inline(never)]
    fn abort(&mut self, error: Error) -> Error {
        self.current_statement = None;
        self.state = State::Done;
        error
    }

    /// Steps the active row-returning statement whose column labels are already initialized.
    #[inline(always)]
    fn do_step_current_prepared(&mut self) -> Result<Option<SQLiteRow>> {
        let prepared = match self.query.as_mut() {
            Query::Prepared(prepared) => Some(prepared),
            Query::Raw(_) => self.current_statement.as_mut(),
        };
        let Some(prepared) = prepared else {
            hint::cold_path();
            return Err(anyhow!("Active SQLite stream has no statement"));
        };
        prepared.do_step_row(false)
    }

    /// Releases the current statement and moves to the next state.
    fn finish_statement(&mut self) {
        match self.query.as_mut() {
            Query::Prepared(prepared) => {
                let _ = prepared.clear_bindings();
                self.state = State::Done;
            }
            Query::Raw(_) => {
                self.current_statement = None;
                self.state = State::Next;
            }
        }
    }

    /// Starts the next statement, yielding its affected count or first row.
    #[cold]
    #[inline(never)]
    fn prepare_next_statement(&mut self) -> Result<Option<QueryResult<SQLiteRow>>> {
        loop {
            if self.state == State::Done {
                return Ok(None);
            }
            let prepared = match self.query.as_mut() {
                Query::Raw(RawQuery(sql)) => loop {
                    let rest = sql.get(self.offset..).unwrap_or_default();
                    if rest.is_empty() {
                        self.state = State::Done;
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
                        break self.current_statement.insert(statement);
                    }
                },
                Query::Prepared(prepared) => {
                    if prepared.is_empty() {
                        self.state = State::Done;
                        return Ok(None);
                    }
                    if !prepared.belongs_to(self.connection) {
                        hint::cold_path();
                        let error =
                            anyhow!("Prepared statement belongs to another SQLite connection");
                        log::error!("{error:#}");
                        return Err(error);
                    }
                    prepared
                }
            };
            match prepared.start()? {
                Some(QueryResult::Row(row)) => {
                    self.state = State::Stepping;
                    return Ok(Some(QueryResult::Row(row)));
                }
                Some(result) => {
                    self.finish_statement();
                    return Ok(Some(result));
                }
                None => self.finish_statement(),
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
        if this.state == State::Stepping {
            match this.do_step_current_prepared() {
                Ok(Some(row)) => return Poll::Ready(Some(Ok(QueryResult::Row(row)))),
                Ok(None) => this.finish_statement(),
                Err(error) => return Poll::Ready(Some(Err(this.abort(error)))),
            }
        }
        if this.state == State::Done {
            Poll::Ready(None)
        } else {
            match this.prepare_next_statement() {
                Ok(item) => Poll::Ready(item.map(Ok)),
                Err(error) => Poll::Ready(Some(Err(this.abort(error)))),
            }
        }
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Drop for SQLiteStream<'_, Q> {
    fn drop(&mut self) {
        if let Query::Prepared(prepared) = self.query.as_mut() {
            let _ = prepared.clear_bindings();
        }
    }
}
