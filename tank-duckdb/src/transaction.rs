use crate::{DuckDBConnection, DuckDBDriver};
use std::future::{self, Future};
use tank_core::{Result, Transaction, impl_executor_transaction};

/// Wrapper for DuckDB transactions.
pub struct DuckDBTransaction<'c> {
    connection: &'c mut DuckDBConnection,
}

impl<'c> DuckDBTransaction<'c> {
    pub async fn new(connection: &'c mut DuckDBConnection) -> Result<Self> {
        connection.exec(c"BEGIN")?;
        Ok(Self { connection })
    }
}

impl_executor_transaction!(DuckDBDriver, DuckDBTransaction<'c>, connection);
impl<'c> Transaction<'c> for DuckDBTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        future::ready(self.connection.exec(c"COMMIT"))
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        future::ready(self.connection.exec(c"ROLLBACK"))
    }
}
