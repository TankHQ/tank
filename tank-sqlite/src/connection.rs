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
    AnyQuery, AsEntity, AsQuery, ColumnDef, Connection, Driver, Entity, Error, ErrorContext,
    Executor, Operand, Prepared, QueryResult, Result, Row, RowsAffected, Sql, Transaction,
    describe_path, error_message_from_ptr, stream::Stream, truncate_long,
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
    pub(crate) fn last_error(handle: *mut sqlite3) -> Error {
        unsafe {
            anyhow!(
                "Error [{}] {}",
                sqlite3_errcode(handle),
                error_message_from_ptr(&sqlite3_errmsg(handle)),
            )
        }
    }

    /// Prepares the first statement in `sql`, returning it and the bytes it consumed.
    pub(crate) fn prepare_statement<const SINGLE_STATEMENT: bool>(
        &mut self,
        sql: &str,
    ) -> Result<(SQLitePrepared, usize)> {
        let (prepared, consumed) = unsafe {
            let make_context = || format!("While preparing the query:\n{}", truncate_long(sql));
            let len = c_int::try_from(sql.len())?;
            let ptr = sql.as_ptr().cast();
            let mut statement = CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                let handle = sqlite3_db_handle(p);
                if sqlite3_finalize(p) != SQLITE_OK {
                    hint::cold_path();
                    log::error!(
                        "{:#}",
                        Self::last_error(handle).context("While finalizing a prepared statement")
                    );
                }
            });
            let mut tail = ptr::null();
            ensure_rc!(
                sqlite3_prepare_v2(*self.handle, ptr, len, &mut *statement, &mut tail),
                Self::last_error(*self.handle),
                make_context(),
            );
            // SQLite stops after the first statement's semicolon or at a NUL byte.
            let consumed = (tail as usize).wrapping_sub(ptr as usize);
            // A prepared statement must hold one, unlike the stream walking empty statements.
            if SINGLE_STATEMENT && statement.is_null() {
                hint::cold_path();
                let error = anyhow!("No statement to prepare").context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            // Anything left after the statement must parse as no further statement.
            if SINGLE_STATEMENT && consumed != sql.len() {
                hint::cold_path();
                // The rest must parse as no statement: whitespace, comments or empty statements.
                check_rest(*self.handle, sql.get(consumed..).unwrap_or_default())
                    .with_context(make_context)
                    .inspect_err(|error| log::error!("{error:#}"))?;
            }
            (statement, consumed)
        };

        #[inline(never)]
        fn check_rest(handle: *mut sqlite3, sql: &str) -> Result<()> {
            let mut statement = CBox::new(ptr::null_mut(), |p| unsafe {
                sqlite3_finalize(p);
            });
            let mut tail = ptr::null();
            if unsafe {
                sqlite3_prepare_v2(
                    handle,
                    sql.as_ptr().cast(),
                    c_int::try_from(sql.len())?,
                    &mut *statement,
                    &mut tail,
                )
            } == SQLITE_OK
                && statement.is_null()
                && tail.cast() == sql.as_ptr().wrapping_add(sql.len())
            {
                Ok(())
            } else {
                Err(anyhow!(
                    "Expected exactly one statement (remaining: {})",
                    truncate_long(sql)
                ))
            }
        }

        Ok((SQLitePrepared::new(prepared), consumed))
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
            Self::last_error(*self.handle),
            format!(
                "While executing:\n{}",
                truncate_long(&statement.to_string_lossy()),
            )
        );
        Ok(())
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
                    let error = Self::last_error(p).context("While closing the sqlite connection");
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
                Self::last_error(*handle),
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
        self.prepare_statement::<true>(sql)
            .map(|(prepared, _)| prepared)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<SQLiteDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        SQLiteStream::new(self, query.as_query(&self.driver()))
    }

    async fn append<It>(&mut self, rows: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        const ROWS: usize = 256;
        let table = <It::Item as AsEntity>::Entity::table();
        let mut rows = rows.into_iter();
        let mut affected = RowsAffected::default();
        let mut prepared = {
            let mut out = mem::take(&mut self.draft);
            out.clear(); // Clear the content, keep the allocated memory
            Sql()
                .insert_into(table)
                .columns(table.columns.iter().map(ColumnDef::name))
                .values([iter::repeat_n(&Operand::QuestionMark, table.columns.len())])
                .write_into(&self.driver().sql_writer(), &mut out);
            let prepared = self
                .prepare_statement::<true>(out.buffer())
                .map(|(prepared, _)| prepared);
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
                    prepared.step_done()?;
                    prepared.clear_bindings()?;
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                affected.extend([RowsAffected {
                    rows_affected: Some(count),
                    last_affected_id: Some(prepared.last_insert_rowid()),
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
