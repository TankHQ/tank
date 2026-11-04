use crate::{MSSQLConnection, MSSQLDriver};
use tank_core::{
    Driver, DynQuery, Executor, Result, SqlWriter, Transaction, future::TryFutureExt,
    impl_executor_transaction,
};

/// Transaction adapter for Microsoft SQL Server.
pub struct MSSQLTransaction<'c> {
    connection: &'c mut MSSQLConnection,
}

impl<'c> MSSQLTransaction<'c> {
    pub async fn new(connection: &'c mut MSSQLConnection) -> Result<Self> {
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

impl_executor_transaction!(MSSQLDriver, MSSQLTransaction<'c>, connection);

impl<'c> Transaction<'c> for MSSQLTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_commit(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_rollback(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }
}
