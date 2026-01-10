use crate::{SQLiteConnection, SQLiteDriver};
use tank_core::{
    Driver, Executor, RawQuery, Result, SqlWriter, Transaction, future::TryFutureExt,
    impl_executor_transaction,
};

pub struct SQLiteTransaction<'c> {
    connection: &'c mut SQLiteConnection,
}

impl<'c> SQLiteTransaction<'c> {
    pub async fn new(connection: &'c mut SQLiteConnection) -> Result<Self> {
        let result = Self { connection };
        let mut query = RawQuery::default();
        result
            .connection
            .driver()
            .sql_writer()
            .write_transaction_begin(&mut query);
        result.connection.execute(query).await?;
        Ok(result)
    }
}

impl_executor_transaction!(SQLiteDriver, SQLiteTransaction<'c>, connection);
impl<'c> Transaction<'c> for SQLiteTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> {
        let mut query = RawQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_commit(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }

    fn rollback(self) -> impl Future<Output = Result<()>> {
        let mut query = RawQuery::default();
        self.driver()
            .sql_writer()
            .write_transaction_rollback(&mut query);
        self.connection.execute(query).map_ok(|_| ())
    }
}
