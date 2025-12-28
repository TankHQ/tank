use crate::{DuckDBConnection, DuckDBDriver};
use tank_core::{
    Driver, Executor, Result, SqlWriter, Transaction, future::TryFutureExt,
    impl_executor_transaction,
};

pub struct DuckDBTransaction<'c> {
    connection: &'c mut DuckDBConnection,
}

impl<'c> DuckDBTransaction<'c> {
    pub async fn new(connection: &'c mut DuckDBConnection) -> Result<Self> {
        let result = Self { connection };
        let mut sql = String::new();
        result
            .connection
            .driver()
            .sql_writer()
            .write_transaction_begin(&mut sql);
        result.connection.execute(sql).await?;
        Ok(result)
    }
}

impl_executor_transaction!(DuckDBDriver, DuckDBTransaction<'c>, connection);
impl<'c> Transaction<'c> for DuckDBTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> {
        let mut sql = String::new();
        self.driver()
            .sql_writer()
            .write_transaction_commit(&mut sql);
        self.connection.execute(sql).map_ok(|_| ())
    }

    fn rollback(self) -> impl Future<Output = Result<()>> {
        let mut sql = String::new();
        self.driver()
            .sql_writer()
            .write_transaction_rollback(&mut sql);
        self.connection.execute(sql).map_ok(|_| ())
    }
}
