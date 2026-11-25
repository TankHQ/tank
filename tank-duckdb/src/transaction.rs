use crate::{DuckDBConnection, DuckDBDriver};
use std::future::{self, Future};
use tank_core::{Result, Transaction, impl_executor_transaction};

/// Wrapper for DuckDB transactions.
pub struct DuckDBTransaction<'c> {
    connection: &'c mut DuckDBConnection,
    is_done: bool,
}

impl<'c> DuckDBTransaction<'c> {
    pub(crate) fn new(connection: &'c mut DuckDBConnection) -> Result<Self> {
        connection.do_execute(c"BEGIN;")?;
        Ok(Self {
            connection,
            is_done: false,
        })
    }
}

impl<'c> Transaction<'c> for DuckDBTransaction<'c> {
    fn commit(mut self) -> impl Future<Output = Result<()>> + Send {
        let result = self.connection.do_execute(c"COMMIT;");
        if result.is_ok() {
            self.is_done = true;
        }
        future::ready(result)
    }

    fn rollback(mut self) -> impl Future<Output = Result<()>> + Send {
        self.is_done = true;
        future::ready(self.connection.do_execute(c"ROLLBACK;"))
    }
}

impl_executor_transaction!(DuckDBDriver, DuckDBTransaction<'c>, connection);

/// Rolls back a transaction dropped without being committed or rolled back.
impl Drop for DuckDBTransaction<'_> {
    fn drop(&mut self) {
        if !self.is_done
            && let Err(error) = self.connection.do_execute(c"ROLLBACK;")
        {
            log::error!(
                "{:#}",
                error.context("While rolling back an unfinished transaction")
            );
        }
    }
}
