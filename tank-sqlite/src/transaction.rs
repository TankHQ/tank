use crate::{SQLiteConnection, SQLiteDriver, sql_writer::SQLiteSqlWriter};
use std::{
    ffi::CString,
    future::{self, Future},
    mem,
    sync::LazyLock,
};
use tank_core::{DynQuery, Result, SqlWriter, Transaction, impl_executor_transaction, log_error};

pub struct SQLiteTransaction<'c> {
    connection: &'c mut SQLiteConnection,
    is_done: bool,
}

impl<'c> SQLiteTransaction<'c> {
    pub(crate) fn new(connection: &'c mut SQLiteConnection) -> Result<Self> {
        static BEGIN: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            let writer = SQLiteSqlWriter {};
            writer.write_transaction_begin(&mut out);
            unsafe { CString::from_vec_unchecked(mem::take(out.buffer().as_mut_vec())) }
        });
        let transaction = Self {
            connection,
            is_done: false,
        };
        transaction.connection.do_execute(&BEGIN)?;
        Ok(transaction)
    }

    fn do_commit(&mut self) -> Result<()> {
        static COMMIT: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            let writer = SQLiteSqlWriter {};
            writer.write_transaction_commit(&mut out);
            unsafe { CString::from_vec_unchecked(mem::take(out.buffer().as_mut_vec())) }
        });
        self.connection.do_execute(&COMMIT)?;
        self.is_done = true;
        Ok(())
    }

    fn do_rollback(&mut self) -> Result<()> {
        static COMMIT: LazyLock<CString> = LazyLock::new(|| {
            let mut out = DynQuery::default();
            let writer = SQLiteSqlWriter {};
            writer.write_transaction_commit(&mut out);
            unsafe { CString::from_vec_unchecked(mem::take(out.buffer().as_mut_vec())) }
        });
        self.connection.do_execute(&COMMIT)?;
        self.is_done = true;
        Ok(())
    }
}

/// Rolls back a transaction dropped without being committed or rolled back.
impl Drop for SQLiteTransaction<'_> {
    fn drop(&mut self) {
        if !self.is_done
            && let Err(error) = self.do_rollback()
        {
            log_error(&error.context("While rolling back an unfinished transaction"));
        }
    }
}

impl_executor_transaction!(SQLiteDriver, SQLiteTransaction<'c>, connection);
impl<'c> Transaction<'c> for SQLiteTransaction<'c> {
    fn commit(mut self) -> impl Future<Output = Result<()>> + Send {
        future::ready(self.do_commit())
    }

    fn rollback(mut self) -> impl Future<Output = Result<()>> + Send {
        future::ready(self.do_rollback())
    }
}
