use crate::{CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction, stream::SQLiteStream};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_int},
    future::{self, Future},
    hint, iter, ptr,
    str::FromStr,
};
use tank_core::{
    AsEntity, AsQuery, ColumnDef, Connection, Driver, DynQuery, Entity, Error, ErrorContext,
    Executor, Operand, Prepared, Query, QueryResult, Result, RowsAffected, Sql, SqlWriter,
    Transaction, describe_path, error_message_from_ptr, future::Either, truncate_long,
};
use tokio::task;

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) connection: CBox<*mut sqlite3>,
}

impl SQLiteConnection {
    pub(crate) fn do_execute(&mut self, statement: &CStr) -> Result<()> {
        let rc = unsafe {
            sqlite3_exec(
                *self.connection,
                statement.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            hint::cold_path();
            return Err(Error::msg(self.last_error()).context(format!(
                "While executing {} (rc {rc})",
                truncate_long!(statement.to_string_lossy())
            )));
        }
        Ok(())
    }

    pub fn last_error(&self) -> String {
        unsafe {
            let errcode = sqlite3_errcode(*self.connection);
            format!(
                "Error ({errcode}): {}",
                error_message_from_ptr(sqlite3_errmsg(*self.connection)),
            )
        }
    }

    /// Inserts every entity in `rows` through a single prepared statement.
    async fn append_entities<I>(&mut self, rows: I) -> Result<RowsAffected>
    where
        I: IntoIterator,
        I::Item: AsEntity,
    {
        type E<I> = <<I as IntoIterator>::Item as AsEntity>::Entity;
        let columns = E::<I>::columns();
        let sql = {
            let mut sql = DynQuery::default();
            self.driver().sql_writer().write_insert(
                &mut sql,
                Sql()
                    .insert_into(E::<I>::table())
                    .columns(columns.iter().map(ColumnDef::name))
                    .values([iter::repeat_n(&Operand::QuestionMark, columns.len())]),
            );
            CString::new(sql.as_str().as_ref())?
        };
        let mut statement = unsafe {
            let mut statement =
                SQLitePrepared::new(CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                    if !p.is_null() {
                        sqlite3_finalize(p);
                    }
                }));
            let rc = sqlite3_prepare_v2(
                *self.connection,
                sql.as_ptr(),
                -1,
                &mut *statement.statement,
                ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                hint::cold_path();
                return Err(self.prepare_error(rc, sql.as_c_str().to_string_lossy()));
            }
            statement
        };
        // A transaction owned by this call keeps file databases from syncing per row.
        let transaction = if unsafe { sqlite3_get_autocommit(*self.connection) } == 0 {
            None
        } else {
            Some(SQLiteTransaction::new(self)?)
        };
        let mut affected = 0u64;
        let mut rows = rows.into_iter();
        loop {
            let appended = rows.by_ref().take(256).try_fold(0u64, |count, entity| {
                for (index, value) in entity.as_entity().row_values().enumerate() {
                    statement.bind_index(value, index as _)?;
                }
                let rc = unsafe { sqlite3_step(statement.statement()) };
                if rc != SQLITE_DONE {
                    hint::cold_path();
                    return Err(Self::step_error(&statement, rc));
                }
                unsafe {
                    sqlite3_clear_bindings(statement.statement());
                    sqlite3_reset(statement.statement());
                }
                Ok(count + 1)
            })?;
            affected += appended;
            if appended < 256 {
                break;
            }
            task::yield_now().await;
        }
        let last_affected_id =
            unsafe { sqlite3_last_insert_rowid(sqlite3_db_handle(statement.statement())) };
        if let Some(transaction) = transaction {
            transaction.commit().await?;
        }
        Ok(RowsAffected {
            rows_affected: Some(affected),
            last_affected_id: Some(last_affected_id),
        })
    }

    #[cold]
    #[inline(never)]
    fn prepare_error(&self, rc: c_int, sql: Cow<'_, str>) -> Error {
        Error::msg(self.last_error()).context(format!(
            "While preparing (rc {rc}):\n{}",
            truncate_long!(sql)
        ))
    }

    #[cold]
    #[inline(never)]
    fn step_error(statement: &SQLitePrepared, rc: c_int) -> Error {
        Error::msg(statement.last_error())
            .context(format!("While stepping a batched insert (rc {rc})"))
    }
}

impl Connection for SQLiteConnection {
    async fn connect(driver: &SQLiteDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        // Must run before SQLite initializes; memory statistics cost a counter update
        // on every allocation and nothing in this driver reads them.
        static CONFIGURE: std::sync::Once = std::sync::Once::new();
        CONFIGURE.call_once(|| unsafe {
            sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0 as c_int);
        });
        let make_context = || format!("While trying to connect to SQLite {}", describe_path(&url));
        let connection_string = CString::from_str(&url.as_str().replacen("sqlite://", "file:", 1))
            .with_context(make_context)?;
        let mut connection;
        unsafe {
            connection = CBox::new(ptr::null_mut(), |p| {
                if sqlite3_close(p) != SQLITE_OK {
                    let error = Error::msg(error_message_from_ptr(sqlite3_errmsg(p)).to_string())
                        .context("While closing the sqlite connection");
                    log::error!("{error:#}");
                }
            });
            // The connection is only reachable through `&mut` borrows, so SQLite's own
            // mutex would only add an uncontended lock to every column read.
            let rc = sqlite3_open_v2(
                connection_string.as_ptr(),
                &mut *connection,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX,
                ptr::null(),
            );
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(sqlite3_errmsg(*connection)).to_string())
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
                    Error::msg(error_message_from_ptr(sqlite3_errmsg(*connection)).to_string())
                        .context("While enabling foreign key enforcement");
                log::error!("{error:#}");
            }
        }
        Ok(Self { connection })
    }

    fn begin(&mut self) -> impl Future<Output = Result<SQLiteTransaction<'_>>> + Send {
        future::ready(SQLiteTransaction::new(self))
    }
}

impl Executor for SQLiteConnection {
    type Driver = SQLiteDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<SQLiteDriver>> {
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
                    let error = Error::msg(error_message_from_ptr(sqlite3_errmsg(db)).to_string())
                        .context("While finalizing a prepared statement");
                    log::error!("{error:#}");
                }
            });
            let mut tail = ptr::null();
            let rc = sqlite3_prepare_v2(connection, sql.as_ptr(), len, &mut *statement, &mut tail);
            if rc != SQLITE_OK {
                let error =
                    Error::msg(error_message_from_ptr(sqlite3_errmsg(connection)).to_string())
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

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<SQLiteDriver> + 's,
    ) -> impl tank_core::stream::Stream<Item = Result<QueryResult<impl tank_core::Row + 's>>> + Send
    {
        SQLiteStream::new(self, query.as_query())
    }

    fn append<It>(&mut self, rows: It) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let rows = rows.into_iter();
        if E::<It>::columns().is_empty() {
            // Without named columns the batch must go through the generic insert.
            let entities = rows.collect::<Vec<_>>();
            let mut query = DynQuery::default();
            self.driver().sql_writer().write_insert(
                &mut query,
                Sql()
                    .insert_into(E::<It>::table())
                    .values(entities.iter().map(AsEntity::as_entity)),
            );
            Either::Right(self.execute(query))
        } else {
            Either::Left(self.append_entities(rows))
        }
    }
}
