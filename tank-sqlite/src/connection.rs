use crate::{
    CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction, ensure_rc, stream::SQLiteStream,
};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_int},
    future::{self, Future},
    hint, iter, mem, ptr,
};
use tank_core::{
    AnyQuery, AppendDefault, AsEntity, AsQuery, ColumnDef, Connection, Entity, Error, ErrorContext,
    Executor, Operand, Prepared, QueryResult, Result, RowsAffected, Sql, Transaction,
    char_ptr_to_string, describe_path, error_message_from_ptr, future::Either, truncate_long,
};
use tokio::task;

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) handle: CBox<*mut sqlite3>,
    draft: AnyQuery,
}

impl SQLiteConnection {
    #[cold]
    #[inline(never)]
    pub(crate) fn last_error(handle: *mut sqlite3) -> String {
        unsafe {
            let ptr = sqlite3_errmsg(handle);
            if !ptr.is_null() {
                format!(
                    "Error [{}] {}",
                    sqlite3_errcode(handle),
                    error_message_from_ptr(&ptr),
                )
            } else {
                Default::default()
            }
        }
    }

    #[inline(always)]
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
                truncate_long(&statement.to_string_lossy()),
            )
        );
        Ok(())
    }

    /// Inserts entities in `rows` through a single prepared statement.
    async fn do_append<I>(&mut self, rows: I) -> Result<RowsAffected>
    where
        I: IntoIterator,
        I::Item: AsEntity,
    {
        const ROWS: usize = 256;
        let table = <I::Item as AsEntity>::Entity::table();
        let mut rows = rows.into_iter();
        let mut affected = RowsAffected::default();
        let mut prepared = {
            let mut out = mem::take(&mut self.draft);
            out.clear(); // Clear the content, keep the allocated memory
            Sql()
                .insert_into(table)
                .columns(table.columns.iter().map(ColumnDef::name))
                .values([iter::repeat_n(&Operand::QuestionMark, table.columns.len())])
                .build_into(&self.driver(), &mut out);
            let prepared = self.do_prepare(out.buffer()).await;
            self.draft = out;
            prepared?
        };
        let transaction = if unsafe { sqlite3_get_autocommit(*self.handle) } == 0 {
            None
        } else {
            // Start a transaction if not inside one so the append either all succeeds or rolls back.
            Some(self.begin().await?)
        };
        loop {
            // Scoped so `entity` never lives across `.await`, keeping the future `Send`.
            {
                let mut count = 0;
                for entity in rows.by_ref().take(ROWS) {
                    for value in entity.as_entity().row_values() {
                        prepared.bind(value)?;
                    }
                    // `sqlite3_step` reports a finished insert as `SQLITE_DONE`, not `SQLITE_OK`.
                    if unsafe { sqlite3_step(*prepared.statement) } != SQLITE_DONE {
                        hint::cold_path();
                        let db = unsafe { sqlite3_db_handle(*prepared.statement) };
                        let error = Error::msg(Self::last_error(db))
                            .context("While executing a step on a prepared statement");
                        log::error!("{error:#}");
                        return Err(error);
                    }
                    prepared.clear_bindings()?;
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                affected.extend([RowsAffected {
                    rows_affected: Some(count),
                    last_affected_id: Some(unsafe {
                        sqlite3_last_insert_rowid(sqlite3_db_handle(*prepared.statement))
                    }),
                }]);
            }
            task::yield_now().await;
        }
        if let Some(transaction) = transaction {
            transaction.commit().await?;
        }
        Ok(affected)
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
        let mut handle;
        let mut connection;
        unsafe {
            handle = CBox::new(ptr::null_mut(), |p| {
                if sqlite3_close(p) != SQLITE_OK {
                    hint::cold_path();
                    let error = Error::msg(Self::last_error(p))
                        .context("While closing the sqlite connection");
                    log::error!("{error:#}");
                }
            });
            ensure_rc!(
                sqlite3_open_v2(
                    url.as_ptr(),
                    &mut *handle,
                    SQLITE_OPEN_READWRITE
                        | SQLITE_OPEN_CREATE
                        | SQLITE_OPEN_URI
                        | SQLITE_OPEN_NOMUTEX,
                    ptr::null()
                ),
                Error::msg(Self::last_error(*handle)),
                make_context(),
            );
            connection = Self {
                handle,
                draft: Default::default(),
            };
            connection.do_execute(c"PRAGMA foreign_keys = ON")?;
        }
        Ok(connection)
    }

    #[inline(always)]
    fn begin(&mut self) -> impl Future<Output = Result<SQLiteTransaction<'_>>> + Send {
        future::ready(SQLiteTransaction::new(self))
    }
}

impl Executor for SQLiteConnection {
    type Driver = SQLiteDriver;

    async fn do_prepare(&mut self, sql: &str) -> Result<SQLitePrepared> {
        let prepared = unsafe {
            let make_context = || format!("While preparing the query:\n{}", truncate_long(sql));
            let len = c_int::try_from(sql.len())?;
            let sql: CString = CString::new(sql.as_bytes())?;
            let mut statement = CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                let handle = sqlite3_db_handle(p);
                if sqlite3_finalize(p) != SQLITE_OK {
                    hint::cold_path();
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
            if tail != ptr::null() && *tail != 0 {
                hint::cold_path();
                let error = anyhow!(
                    "Cannot prepare multiple statements at a time (remaining: {})",
                    truncate_long(&char_ptr_to_string(tail))
                )
                .context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            statement
        };
        Ok(SQLitePrepared::new(prepared))
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
        let rows = rows.into_iter();
        if <It::Item as AsEntity>::Entity::columns().is_empty() {
            // Without named columns the batch must go through the generic insert.
            Either::Right(self.append_default(rows))
        } else {
            Either::Left(self.do_append(rows))
        }
    }
}
