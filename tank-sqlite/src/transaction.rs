use crate::{SQLiteConnection, SQLiteDriver, sql_writer::SQLiteSqlWriter};
use libsqlite3_sys::{SQLITE_OK, sqlite3_exec};
use std::{
    ffi::{CStr, CString},
    future::Future,
    ptr,
    sync::LazyLock,
};
use tank_core::{DynQuery, Error, Result, SqlWriter, Transaction, impl_executor_transaction};

pub struct SQLiteTransaction<'c> {
    connection: &'c mut SQLiteConnection,
    is_done: bool,
}

impl<'c> SQLiteTransaction<'c> {
    pub async fn new(connection: &'c mut SQLiteConnection) -> Result<Self> {
        static BEGIN: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            SQLiteSqlWriter {}.write_transaction_begin(&mut out);
            CString::new(out.as_str().as_ref()).expect("a transaction statement holds no NUL bytes")
        });
        let transaction = Self {
            connection,
            is_done: false,
        };
        Self::exec(transaction.connection, &BEGIN)?;
        Ok(transaction)
    }

    /// Rolls the transaction back, leaving nothing for `Drop` to redo.
    fn discard(&mut self) -> Result<()> {
        static ROLLBACK: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            SQLiteSqlWriter {}.write_transaction_rollback(&mut out);
            CString::new(out.as_str().as_ref()).expect("a transaction statement holds no NUL bytes")
        });
        self.is_done = true;
        Self::exec(self.connection, &ROLLBACK)
    }

    fn exec(connection: &mut SQLiteConnection, statement: &CStr) -> Result<()> {
        let rc = unsafe {
            sqlite3_exec(
                *connection.connection,
                statement.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(Error::msg(connection.last_error()).context(format!(
                "While executing {} (rc {rc})",
                statement.to_string_lossy()
            )));
        }
        Ok(())
    }
}

/// Rolls back a transaction dropped without being committed or rolled back.
impl Drop for SQLiteTransaction<'_> {
    fn drop(&mut self) {
        if !self.is_done
            && let Err(error) = self.discard()
        {
            log::error!("While rolling back an unfinished transaction: {error:#}");
        }
    }
}

impl_executor_transaction!(SQLiteDriver, SQLiteTransaction<'c>, connection);
impl<'c> Transaction<'c> for SQLiteTransaction<'c> {
    fn commit(mut self) -> impl Future<Output = Result<()>> + Send {
        static COMMIT: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            SQLiteSqlWriter {}.write_transaction_commit(&mut out);
            CString::new(out.as_str().as_ref()).expect("a transaction statement holds no NUL bytes")
        });
        let result = Self::exec(self.connection, &COMMIT);
        self.is_done = result.is_ok();
        std::future::ready(result)
    }

    fn rollback(mut self) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(self.discard())
    }
}
