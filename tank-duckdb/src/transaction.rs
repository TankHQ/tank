use crate::{DuckDBConnection, DuckDBDriver};
use tank_core::{
    DynQuery, Driver, Executor, Result, SqlWriter, Transaction, future::TryFutureExt,
    impl_executor_transaction,
};

/// Wrapper for DuckDB transactions.
///
/// Begins a transaction via the driver `SqlWriter` and executes commit/rollback
/// SQL through the connection. Implements `Transaction` for `tank_core`.
pub struct DuckDBTransaction<'c> {
    connection: &'c mut DuckDBConnection,
}

impl<'c> DuckDBTransaction<'c> {
    pub async fn new(connection: &'c mut DuckDBConnection) -> Result<Self> {
        let result = Self { connection };
        let mut query = DynQuery::default();
        result
            .connection
            .driver()
            .sql_writer()
            .write_transaction_begin(&mut query);
        result.connection.execute(query).await?;
        Ok(result)
    }
}

impl_executor_transaction!(DuckDBDriver, DuckDBTransaction<'c>, connection);
impl<'c> Transaction<'c> for DuckDBTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_commit(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }

    fn rollback(self) -> impl Future<Output = Result<()>> {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_rollback(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }
}
