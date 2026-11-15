use crate::{
    CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction, ensure_rc, stream::SQLiteStream,
};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_int},
    future::{self, Future},
    hint, iter, ptr,
};
use tank_core::{
    AsEntity, AsQuery, ColumnDef, Connection, Driver, DynQuery, Entity, Error, ErrorContext,
    Executor, Operand, Prepared, Query, QueryResult, Result, RowsAffected, Sql, SqlWriter,
    Transaction, char_ptr_to_string, describe_path, error_message_from_ptr, future::Either,
    truncate_long,
};
use tokio::task;

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) handle: CBox<*mut sqlite3>,
}

impl SQLiteConnection {
    pub(crate) fn do_execute(&mut self, statement: &CStr) -> Result<()> {
        ensure_rc!(
            unsafe {
                sqlite3_exec(
                    *self.handle,
                    statement.as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            Error::msg(Self::last_error(*self.handle)),
            format!(
                "While executing:\n{}",
                truncate_long!(statement.to_string_lossy()),
            )
        );
        Ok(())
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
        let mut prepared = unsafe {
            let mut prepared =
                SQLitePrepared::new(CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                    if !p.is_null() {
                        sqlite3_finalize(p);
                    }
                }));
            ensure_rc!(
                sqlite3_prepare_v2(
                    *self.handle,
                    sql.as_ptr(),
                    -1,
                    &mut *prepared.statement,
                    ptr::null_mut(),
                ),
                Error::msg(Self::last_error(*self.handle)),
                "While preparing a batch insert statement"
            );
            prepared
        };
        let transaction = if unsafe { sqlite3_get_autocommit(*self.handle) } == 0 {
            None
        } else {
            Some(SQLiteTransaction::new(self)?)
        };
        let mut affected = 0u64;
        let mut rows = rows.into_iter();
        loop {
            let appended = rows.by_ref().take(256).try_fold(0u64, |count, entity| {
                for (index, value) in entity.as_entity().row_values().enumerate() {
                    prepared.bind_index(value, index as _)?;
                }
                let rc = unsafe { sqlite3_step(*prepared.statement) };
                if rc != SQLITE_DONE {
                    hint::cold_path();
                    return Err(Error::msg(Self::last_error(unsafe {
                        sqlite3_db_handle(*prepared.statement)
                    })))
                    .context("While executing a step on a prepared statement")
                    .inspect_err(|error| log::error!("{error:#}"));
                }
                unsafe {
                    sqlite3_clear_bindings(*prepared.statement);
                    sqlite3_reset(*prepared.statement);
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
            unsafe { sqlite3_last_insert_rowid(sqlite3_db_handle(*prepared.statement)) };
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
    pub(crate) fn last_error(handle: *mut sqlite3) -> String {
        unsafe {
            let ptr = sqlite3_errmsg(handle);
            if !ptr.is_null() {
                format!(
                    "Error {} {}",
                    sqlite3_errcode(handle),
                    error_message_from_ptr(&ptr),
                )
            } else {
                Default::default()
            }
        }
    }
}

impl Connection for SQLiteConnection {
    async fn connect(driver: &SQLiteDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!(r#"Connecting to SQLite "{}""#, describe_path(&url));
        let url = CString::from_vec_with_nul({
            let mut bytes = url.as_str().replacen("sqlite://", "file:", 1).into_bytes();
            bytes.push(0);
            bytes
        })
        .with_context(make_context)?;
        let mut connection;
        unsafe {
            connection = CBox::new(ptr::null_mut(), |p| {
                if sqlite3_close(p) != SQLITE_OK {
                    let error = Error::msg(Self::last_error(p))
                        .context("While closing the sqlite connection");
                    log::error!("{error:#}");
                }
            });
            ensure_rc!(
                sqlite3_open_v2(
                    url.as_ptr(),
                    &mut *connection,
                    SQLITE_OPEN_READWRITE
                        | SQLITE_OPEN_CREATE
                        | SQLITE_OPEN_URI
                        | SQLITE_OPEN_NOMUTEX,
                    ptr::null()
                ),
                Error::msg(Self::last_error(*connection)),
                make_context(),
            );
            ensure_rc!(
                sqlite3_exec(
                    *connection,
                    c"PRAGMA foreign_keys = ON".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                Error::msg(Self::last_error(*connection)),
                "While enabling foreign key enforcement",
            );
        }
        Ok(Self { handle: connection })
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
            let len = c_int::try_from(sql.len())?;
            let sql = CString::new(sql.as_bytes())?;
            let mut statement = CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                let handle = sqlite3_db_handle(p);
                if sqlite3_finalize(p) != SQLITE_OK {
                    log::error!(
                        "{:#}",
                        Error::msg(Self::last_error(handle))
                            .context("While finalizing a prepared statement")
                    );
                }
            });
            let mut tail = ptr::null();
            ensure_rc!(
                sqlite3_prepare_v2(*self.handle, sql.as_ptr(), len, &mut *statement, &mut tail),
                Error::msg(Self::last_error(*self.handle)),
                make_context(),
            );
            if tail != ptr::null() && *tail != '\0' as i8 {
                let error = anyhow!(
                    "Cannot prepare more than one statement at a time (remaining: {})",
                    truncate_long!(char_ptr_to_string(tail))
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
