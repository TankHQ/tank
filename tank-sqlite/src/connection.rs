use crate::{
    RawDb, SQLiteDriver, SQLitePrepared, SQLiteTransaction, ensure_rc,
    stream::{SQLiteRowStream, SQLiteStream},
    yield_now,
};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_int},
    future::{self, Future},
    hint, iter, mem, ptr,
    sync::{Arc, Mutex, PoisonError},
};
use tank_core::{
    AnyQuery, AsEntity, AsQuery, ColumnDef, Connection, Driver, Entity, ErrorContext, Executor,
    Operand, QueryResult, Result, Row, RowsAffected, Sql, TableRef, Transaction, describe_path,
    stream::Stream, truncate_long,
};

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) db: Arc<Mutex<RawDb>>,
    draft: AnyQuery,
}

impl SQLiteConnection {
    const DEFAULT_BUSY_TIMEOUT_MS: c_int = 5_000;

    #[inline(always)]
    pub(crate) fn is_autocommit(&self) -> bool {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        unsafe { sqlite3_get_autocommit(db.as_ptr()) != 0 }
    }

    /// Prepares the first statement in `sql`, returning it and the bytes it consumed.
    pub(crate) fn prepare_statement<const SINGLE_STATEMENT: bool, const PERSISTENT: bool>(
        &mut self,
        sql: &str,
    ) -> Result<(SQLitePrepared, usize)> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        let make_context = || format!("While preparing the query:\n{}", truncate_long(sql));
        let len = c_int::try_from(sql.len())?;
        let ptr = sql.as_ptr() as _;
        let flags = if PERSISTENT {
            SQLITE_PREPARE_PERSISTENT
        } else {
            0
        };
        let mut statement = ptr::null_mut();
        let mut tail = ptr::null();
        ensure_rc!(
            unsafe {
                sqlite3_prepare_v3(db.as_ptr(), ptr, len, flags, &mut statement, &mut tail)
            },
            db.last_error(),
            make_context(),
        );
        // SQLite stops after the first statement's semicolon or at a NUL byte.
        let consumed = (tail as usize).wrapping_sub(ptr as usize);
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
            if let Err(error) = check_rest(&db, sql.get(consumed..).unwrap_or_default()) {
                db.finalize(statement);
                let error = error.context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            #[inline(never)]
            fn check_rest(db: &RawDb, sql: &str) -> Result<()> {
                let mut statement = ptr::null_mut();
                let mut tail = ptr::null();
                let rc = unsafe {
                    sqlite3_prepare_v2(
                        db.as_ptr(),
                        sql.as_ptr() as _,
                        c_int::try_from(sql.len())?,
                        &mut statement,
                        &mut tail,
                    )
                };
                let is_null = statement.is_null();
                if !is_null {
                    db.finalize(statement);
                }
                if rc == SQLITE_OK
                    && is_null
                    && tail as *const u8 == sql.as_ptr().wrapping_add(sql.len())
                {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Expected exactly one statement (remaining: {})",
                        truncate_long(sql)
                    ))
                }
            }
        }
        Ok((
            SQLitePrepared::new(self.db.clone(), statement),
            consumed,
        ))
    }

    #[inline(always)]
    pub(crate) fn do_execute(&mut self, statement: &CStr) -> Result<()> {
        let db = self.db.lock().unwrap_or_else(PoisonError::into_inner);
        ensure_rc!(
            unsafe {
                sqlite3_exec(
                    db.as_ptr(),
                    statement.as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            db.last_error(),
            format!(
                "While executing:\n{}",
                truncate_long(&statement.to_string_lossy()),
            )
        );
        Ok(())
    }

    fn prepare_append(
        &mut self,
        table: &'static TableRef,
    ) -> Result<(SQLitePrepared, Option<SQLiteTransaction<'_>>)> {
        let mut out = mem::take(&mut self.draft);
        out.clear(); // Clear the content, keep the allocated memory
        Sql()
            .insert_into(table)
            .columns(table.columns.iter().map(ColumnDef::name))
            .values([iter::repeat_n(&Operand::QuestionMark, table.columns.len())])
            .write_into(&self.driver().sql_writer(), &mut out);
        let prepared = self
            .prepare_statement::<true, false>(out.buffer())
            .map(|(prepared, _)| prepared);
        self.draft = out;
        let prepared = prepared?;
        let transaction = if self.is_autocommit() {
            // Start a transaction if not inside one so the append either all succeeds or rolls back.
            Some(SQLiteTransaction::new(self)?)
        } else {
            None
        };
        Ok((prepared, transaction))
    }
}

impl Connection for SQLiteConnection {
    async fn connect(driver: &SQLiteDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!(r#"Connecting to SQLite "{}""#, describe_path(&url));
        let mut busy_timeout_ms = Self::DEFAULT_BUSY_TIMEOUT_MS;
        for (key, value) in url.query_pairs() {
            if key == "busy_timeout" {
                let Some(ms) = value.parse::<c_int>().ok().filter(|&ms| ms >= 0) else {
                    hint::cold_path();
                    let error = anyhow!(
                        "Invalid `busy_timeout` value {value:?}, expected a non-negative integer in milliseconds"
                    )
                    .context(make_context());
                    log::error!("{error:#}");
                    return Err(error);
                };
                busy_timeout_ms = ms;
            }
        }
        let url = CString::from_vec_with_nul({
            let mut bytes = url.as_str().replacen("sqlite://", "file:", 1).into_bytes();
            bytes.push(0);
            bytes
        })
        .with_context(make_context)?;
        let mut raw = RawDb::null();
        unsafe {
            ensure_rc!(
                sqlite3_open_v2(
                    url.as_ptr(),
                    raw.as_mut_ptr(),
                    SQLITE_OPEN_READWRITE
                        | SQLITE_OPEN_CREATE
                        | SQLITE_OPEN_URI
                        | SQLITE_OPEN_NOMUTEX,
                    ptr::null()
                ),
                raw.last_error(),
                make_context(),
            );
            ensure_rc!(
                sqlite3_busy_timeout(raw.as_ptr(), busy_timeout_ms),
                raw.last_error(),
                make_context(),
            );
        }
        let mut connection = Self {
            db: Arc::new(Mutex::new(raw)),
            draft: Default::default(),
        };
        connection.do_execute(c"PRAGMA foreign_keys = ON")?;
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
        self.prepare_statement::<true, true>(sql)
            .map(|(prepared, _)| prepared)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<SQLiteDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        SQLiteStream::new(self, query.as_query(&self.driver()))
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<SQLiteDriver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        SQLiteRowStream::new(self, query.as_query(&self.driver()))
    }

    async fn append<It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        const BATCH_SIZE: usize = 256;
        let table = <It::Item as AsEntity>::Entity::table();
        let mut entities = entities.into_iter();
        let mut affected = RowsAffected::default();
        let mut connection = Some(self);
        let mut state = None;
        loop {
            if state.is_some() {
                yield_now().await;
            }
            let Some(first) = entities.next() else {
                break;
            };
            let (prepared, _) = match state.as_mut() {
                Some(state) => state,
                None => {
                    let Some(connection) = connection.take() else {
                        break;
                    };
                    state.insert(connection.prepare_append(table)?)
                }
            };
            let batch = prepared
                .append_batch(iter::once(first).chain(entities.by_ref()).take(BATCH_SIZE))?;
            affected.extend([batch]);
            if batch.rows_affected.unwrap_or_default() < BATCH_SIZE as u64 {
                break;
            }
        }
        if let Some((_, Some(transaction))) = state {
            transaction.commit().await?;
        }
        Ok(affected)
    }
}
