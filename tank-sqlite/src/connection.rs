use crate::{
    CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction,
    extract::{extract_name, extract_value},
};
use async_stream::try_stream;
use flume::Sender;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_int},
    mem, ptr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicPtr, Ordering},
    },
};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Prepared, Query, QueryResult, RawQuery,
    Result, RowLabeled, RowsAffected, error_message_from_ptr, send_value, stream::Stream,
    truncate_long,
};
use tokio::task::spawn_blocking;

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
///
/// Provides helpers to prepare/execute statements and stream results into `tank_core` result types.
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

    pub(crate) fn do_run_prepared(
        connection: *mut sqlite3,
        statement: *mut sqlite3_stmt,
        tx: Sender<Result<QueryResult>>,
    ) {
        unsafe {
            let count = sqlite3_column_count(statement);
            let labels = match (0..count)
                .map(|i| extract_name(statement, i))
                .collect::<Result<Arc<[_]>>>()
            {
                Ok(labels) => labels,
                Err(error) => {
                    send_value!(tx, Err(error.into()));
                    return;
                }
            };
            loop {
                match sqlite3_step(statement) {
                    SQLITE_BUSY => {
                        continue;
                    }
                    SQLITE_DONE => {
                        if sqlite3_stmt_readonly(statement) == 0 {
                            send_value!(
                                tx,
                                Ok(QueryResult::Affected(RowsAffected {
                                    rows_affected: Some(sqlite3_changes64(connection) as _),
                                    last_affected_id: Some(sqlite3_last_insert_rowid(connection)),
                                }))
                            );
                        }
                        break;
                    }
                    SQLITE_ROW => {
                        let values = match (0..count)
                            .map(|i| extract_value(statement, i))
                            .collect::<Result<_>>()
                        {
                            Ok(value) => value,
                            Err(error) => {
                                send_value!(tx, Err(error));
                                return;
                            }
                        };
                        send_value!(
                            tx,
                            Ok(QueryResult::Row(RowLabeled {
                                labels: labels.clone(),
                                values: values,
                            }))
                        )
                    }
                    _ => {
                        send_value!(
                            tx,
                            Err(Error::msg(
                                error_message_from_ptr(&sqlite3_errmsg(sqlite3_db_handle(
                                    statement,
                                )))
                                .to_string(),
                            ))
                        );
                        break;
                    }
                }
            }
        }
    }

    pub(crate) fn do_run_unprepared(
        connection: *mut sqlite3,
        sql: &str,
        tx: Sender<Result<QueryResult>>,
    ) {
        unsafe {
            let sql = sql.trim();
            let mut it = sql.as_ptr() as *const c_char;
            let mut len = sql.len();
            loop {
                let (statement, tail) = {
                    let mut statement = SQLitePrepared::new(CBox::new(ptr::null_mut(), |p| {
                        let rc = sqlite3_finalize(p);
                        if rc != SQLITE_OK {
                            return;
                        }
                    }));
                    let mut sql_tail = ptr::null();
                    let rc = sqlite3_prepare_v2(
                        connection,
                        it,
                        len as c_int,
                        &mut *statement.statement,
                        &mut sql_tail,
                    );
                    if rc != SQLITE_OK {
                        send_value!(
                            tx,
                            Err(Error::msg(
                                error_message_from_ptr(&sqlite3_errmsg(connection)).to_string(),
                            ))
                        );
                        return;
                    }
                    (statement, sql_tail)
                };
                Self::do_run_prepared(connection, statement.statement(), tx.clone());
                len = if tail != ptr::null() {
                    len - tail.offset_from_unsigned(it)
                } else {
                    0
                };
                if len == 0 {
                    break;
                }
                it = tail;
            }
        };
    }
}

impl Executor for SQLiteConnection {
    type Driver = SQLiteDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<Self::Driver>> {
        let connection = AtomicPtr::new(*self.connection);
        let context = format!("While preparing the query:\n{}", truncate_long!(sql));
        let prepared = spawn_blocking(move || unsafe {
            let connection = connection.load(Ordering::Relaxed);
            let len = sql.len();
            let sql = CString::new(sql.into_bytes())?;
            let mut statement = CBox::new(ptr::null_mut(), |p| {
                let db = sqlite3_db_handle(p);
                let rc = sqlite3_finalize(p);
                if rc != SQLITE_OK {
                    let error = Error::msg(error_message_from_ptr(&sqlite3_errmsg(db)).to_string())
                        .context("While finalizing a prepared statement");
                    log::error!("{error:#}");
                }
            });
            let mut tail = ptr::null();
            let rc = sqlite3_prepare_v2(
                connection,
                sql.as_ptr(),
                len as c_int,
                &mut *statement,
                &mut tail,
            );
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(&sqlite3_errmsg(connection)).to_string())
                        .context(context);
                log::error!("{:#}", error);
                return Err(error);
            }
            if tail != ptr::null() && *tail != '\0' as i8 {
                let error = Error::msg(format!(
                    "Cannot prepare more than one statement at a time (remaining: {})",
                    CStr::from_ptr(tail).to_str().unwrap_or("unprintable")
                ))
                .context(context);
                log::error!("{:#}", error);
                return Err(error);
            }
            Ok(statement)
        })
        .await?;
        Ok(SQLitePrepared::new(prepared?).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        let (tx, rx) = flume::unbounded::<Result<QueryResult>>();
        let connection = AtomicPtr::new(*self.connection);
        let mut owned = mem::take(query.as_mut());
        let join = spawn_blocking(move || {
            match &mut owned {
                Query::Raw(RawQuery(sql)) => {
                    Self::do_run_unprepared(connection.load(Ordering::Relaxed), sql, tx);
                }
                Query::Prepared(prepared) => {
                    Self::do_run_prepared(
                        connection.load(Ordering::Relaxed),
                        prepared.statement(),
                        tx,
                    );
                    let _ = prepared.clear_bindings();
                }
            }
            owned
        });
        try_stream! {
            while let Ok(result) = rx.recv_async().await {
                yield result.map_err(|e| {
                    let error = e.context(context.clone());
                    log::error!("{:#}", error);
                    error
                })?;
            }
            *query.as_mut() = mem::take(&mut join.await?);
        }
    }
}

impl Connection for SQLiteConnection {
    async fn connect(url: Cow<'static, str>) -> Result<SQLiteConnection> {
        let context = format!("While trying to connect to `{}`", truncate_long!(url));
        let url = Self::sanitize_url(url)?;
        let url = CString::from_str(&url.as_str().replacen("sqlite://", "file:", 1))
            .with_context(|| context.clone())?;
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
                url.as_ptr(),
                &mut *connection,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                ptr::null(),
            );
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(&sqlite3_errmsg(*connection)).to_string())
                        .context(context);
                log::error!("{:#}", error);
                return Err(error);
            }
        }
        Ok(Self { connection })
    }

    fn begin(&mut self) -> impl Future<Output = Result<SQLiteTransaction<'_>>> {
        SQLiteTransaction::new(self)
    }
}
