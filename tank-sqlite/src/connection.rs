use crate::{CBox, SQLiteDriver, SQLitePrepared, SQLiteTransaction};
use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_int},
    future::Future,
    hint, ptr,
    str::FromStr,
    sync::Arc,
};
use tank_core::{
    AsEntity, AsQuery, Connection, Context, Driver, DynQuery, Entity, Error, ErrorContext,
    Executor, Fragment, Prepared, Query, QueryResult, Result, RowOwned, RowsAffected, Sql,
    SqlCoreWriter, SqlWriter, describe_path, error_message_from_ptr, future::Either, truncate_long,
};

/// Wrapper for a SQLite `sqlite3` connection pointer used by the SQLite driver.
#[derive(Debug)]
pub struct SQLiteConnection {
    pub(crate) connection: CBox<*mut sqlite3>,
}

impl SQLiteConnection {
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
    fn append_entities<I>(&mut self, rows: I) -> Result<RowsAffected>
    where
        I: IntoIterator,
        I::Item: AsEntity,
    {
        type E<I> = <<I as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<I>::table();
        let columns = E::<I>::columns();
        let writer = self.driver().sql_writer();
        let mut sql = DynQuery::with_capacity(32 + columns.len() * 12);
        let mut context = Context::new(Fragment::SqlInsertInto, false);
        sql.push_str("INSERT INTO ");
        writer.write_table_ref(&mut context, &mut sql, table);
        sql.push_str(" (");
        for (i, column) in columns.iter().enumerate() {
            if i != 0 {
                sql.push_str(", ");
            }
            writer.write_identifier(&mut context, &mut sql, column.name(), true);
        }
        sql.push_str(") VALUES (");
        for i in 0..columns.len() {
            if i != 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push(')');
        let sql = CString::new(sql.as_str().as_ref())?;
        let connection = *self.connection;
        unsafe {
            let mut statement =
                SQLitePrepared::new(CBox::new(ptr::null_mut(), |p: *mut sqlite3_stmt| {
                    if !p.is_null() {
                        sqlite3_finalize(p);
                    }
                }));
            let rc = sqlite3_prepare_v2(
                connection,
                sql.as_ptr(),
                -1,
                &mut *statement.statement,
                ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                hint::cold_path();
                return Err(self.prepare_error(rc, sql.as_c_str().to_string_lossy()));
            }
            // An implicit transaction per statement keeps file databases from syncing per row.
            let own_transaction = sqlite3_get_autocommit(connection) != 0;
            if own_transaction
                && sqlite3_exec(
                    connection,
                    c"BEGIN".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ) != SQLITE_OK
            {
                hint::cold_path();
                return Err(self.step_error(SQLITE_ERROR));
            }
            let mut affected = 0u64;
            // Reused for temporal values, which the dialect formats as text.
            for entity in rows {
                let entity = entity.as_entity();
                for (index, value) in entity.row_values().enumerate() {
                    if let Err(error) = statement.bind_index(&value, index as _) {
                        if own_transaction {
                            sqlite3_exec(
                                connection,
                                c"ROLLBACK".as_ptr(),
                                None,
                                ptr::null_mut(),
                                ptr::null_mut(),
                            );
                        }
                        return Err(error);
                    }
                }
                let rc = sqlite3_step(statement.statement());
                if rc != SQLITE_DONE {
                    hint::cold_path();
                    let error = self.step_error(rc);
                    if own_transaction {
                        sqlite3_exec(
                            connection,
                            c"ROLLBACK".as_ptr(),
                            None,
                            ptr::null_mut(),
                            ptr::null_mut(),
                        );
                    }
                    return Err(error);
                }
                sqlite3_clear_bindings(statement.statement());
                sqlite3_reset(statement.statement());
                affected += 1;
            }
            if own_transaction
                && sqlite3_exec(
                    connection,
                    c"COMMIT".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ) != SQLITE_OK
            {
                hint::cold_path();
                return Err(self.step_error(SQLITE_ERROR));
            }
            Ok(RowsAffected {
                rows_affected: Some(affected),
                last_affected_id: Some(sqlite3_last_insert_rowid(connection)),
            })
        }
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
    fn step_error(&self, rc: c_int) -> Error {
        Error::msg(self.last_error()).context(format!("While stepping a batched insert (rc {rc})"))
    }
}

// Both iterator levels own this state. Even a rowset retained after the outer iterator
// is dropped keeps the connection and prepared query borrowed. All native access is
// serialized by the caller's exclusive borrow; the connection opens without a mutex.
struct SQLiteCursor<'s, Q: AsMut<Query<SQLiteDriver>>> {
    connection: &'s mut SQLiteConnection,
    query: Q,
    statement: Option<SQLitePrepared>,
    offset: usize,
    generation: usize,
    columns: c_int,
    active: bool,
    done: bool,
    names: Option<Arc<[String]>>,
}

impl<Q: AsMut<Query<SQLiteDriver>>> SQLiteCursor<'_, Q> {
    /// The statement this cursor reads from, owned for raw SQL or borrowed from the query.
    #[inline(always)]
    fn prepared(&mut self) -> Option<&SQLitePrepared> {
        match self.query.as_mut() {
            Query::Prepared(prepared) => Some(prepared),
            Query::Raw(_) => self.statement.as_ref(),
        }
    }

    #[inline(always)]
    fn statement(&mut self) -> *mut sqlite3_stmt {
        self.prepared()
            .map_or(ptr::null_mut(), SQLitePrepared::statement)
    }

    #[cold]
    #[inline(never)]
    fn error(&mut self) -> Error {
        Error::msg(self.connection.last_error())
            .context(format!("While running the query:\n{}", self.query.as_mut()))
    }

    /// Reads the column labels, which SQLite may only expose after the first step.
    ///
    /// Prepared statements keep them between runs; the count is re-checked so a
    /// recompiled statement never decodes a different shape under old labels.
    #[cold]
    #[inline(never)]
    fn init_names(&mut self) -> Result<Arc<[String]>> {
        let Some(prepared) = self.prepared() else {
            return Ok(Arc::default());
        };
        let columns = prepared.column_count();
        let cached = if prepared.columns == columns {
            prepared.labels.clone()
        } else {
            None
        };
        let names = match cached {
            Some(names) => names,
            None => (0..columns)
                .map(|index| prepared.column_name(index))
                .collect::<Result<Arc<[_]>>>()?,
        };
        if let Query::Prepared(prepared) = self.query.as_mut() {
            prepared.columns = columns;
            prepared.labels = Some(names.clone());
        }
        self.columns = columns;
        self.names = Some(names.clone());
        Ok(names)
    }

    fn reset(&mut self) {
        let statement = self.statement();
        if !statement.is_null() {
            // A step error is already returned to the caller. Reset reports that same
            // error again, but still resets the VM, so do not report it twice.
            unsafe { sqlite3_reset(statement) };
        }
        self.active = false;
        self.columns = 0;
        self.names = None;
        self.statement = None;
    }

    /// Copies the current row out of the statement.
    fn row(&mut self) -> Result<Option<RowOwned>> {
        if !self.active {
            return Ok(None);
        }
        let statement = self.statement();
        let result = unsafe {
            match sqlite3_step(statement) {
                SQLITE_ROW => {
                    let names = if let Some(names) = &self.names {
                        names.clone()
                    } else {
                        hint::cold_path();
                        self.init_names()?
                    };
                    let columns = self.columns;
                    let mut values = Vec::with_capacity(columns as usize);
                    if let Some(prepared) = self.prepared() {
                        for index in 0..columns {
                            values.push(prepared.extract(index)?.into_owned());
                        }
                    }
                    Ok(Some(RowOwned::new(names, values)))
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
                Query::Prepared(_) => {
                    if self.generation != 0 || self.statement().is_null() {
                        self.done = true;
                        return Ok(None);
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
                        *self.connection.connection,
                        sql,
                        len,
                        &mut *statement.statement,
                        &mut tail,
                    );
                    if rc != SQLITE_OK {
                        self.done = true;
                        return Err(self.error());
                    }
                    self.offset += tail.offset_from(sql) as usize;
                    if !statement.statement().is_null() {
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
            let connection = *self.connection.connection;
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
}

impl<Q: AsMut<Query<SQLiteDriver>>> Drop for SQLiteCursor<'_, Q> {
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
        if let Query::Prepared(prepared) = self.query.as_mut() {
            if !prepared.statement().is_null() {
                if let Err(error) = prepared.clear_bindings() {
                    log::error!("{error:#}");
                }
            }
        }
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
        SQLiteTransaction::new(self)
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
        let mut query = query.as_query();
        if let Query::Raw(raw) = query.as_mut() {
            if raw.0.as_bytes().contains(&0) {
                return tank_core::future::Either::Left(tank_core::stream::once(
                    std::future::ready(Err(anyhow!("SQLite query contains an interior NUL byte"))),
                ));
            }
        }
        if let Query::Prepared(prepared) = query.as_mut() {
            if !prepared.statement().is_null()
                && unsafe { sqlite3_db_handle(prepared.statement()) } != *self.connection
            {
                return tank_core::future::Either::Left(tank_core::stream::once(
                    std::future::ready(Err(anyhow!(
                        "Prepared statement belongs to another SQLite connection"
                    ))),
                ));
            }
        }
        tank_core::future::Either::Right(tank_core::stream::iter(SQLiteCursor {
            connection: self,
            query,
            statement: None,
            offset: 0,
            generation: 0,
            columns: 0,
            active: false,
            done: false,
            names: None,
        }))
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
            Either::Left(std::future::ready(self.append_entities(rows)))
        }
    }
}

impl<Q: AsMut<Query<SQLiteDriver>>> Iterator for SQLiteCursor<'_, Q> {
    type Item = Result<QueryResult<RowOwned>>;

    fn next(&mut self) -> Option<Self::Item> {
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
