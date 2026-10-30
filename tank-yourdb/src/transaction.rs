use crate::{YourDBConnection, YourDBDriver};
use tank_core::{Error, Result, Transaction, impl_executor_transaction};

pub struct YourDBTransaction<'c> {
    connection: &'c mut YourDBConnection,
}

impl_executor_transaction!(YourDBDriver, YourDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for YourDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
    async fn rollback(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
}
