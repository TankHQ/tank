use crate::{ScyllaDBConnection, ScyllaDBDriver};
use tank_core::{Error, Result, Transaction, impl_executor_transaction};

pub struct ScyllaDBTransaction<'c> {
    connection: &'c mut ScyllaDBConnection,
}

impl_executor_transaction!(ScyllaDBDriver, ScyllaDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for ScyllaDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by ScyllaDB"))
    }

    async fn rollback(self) -> Result<()> {
        Err(Error::msg("Transactions are not supported by ScyllaDB"))
    }
}
