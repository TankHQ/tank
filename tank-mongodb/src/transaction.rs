use crate::{MongoDBConnection, MongoDBDriver};
use tank_core::{Error, Result, Transaction, impl_executor_transaction};

pub struct MongoDBTransaction<'c> {
    connection: &'c mut MongoDBConnection,
}

impl_executor_transaction!(MongoDBDriver, MongoDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for MongoDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by this MongoDB driver"))
    }
    async fn rollback(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by this MongoDB driver"))
    }
}
