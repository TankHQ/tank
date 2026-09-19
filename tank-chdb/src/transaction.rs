use crate::{ChDBConnection, ChDBDriver};
use anyhow::anyhow;
use tank_core::{Result, Transaction, impl_executor_transaction};

/// chDB transaction wrapper.
pub struct ChDBTransaction<'c> {
    connection: &'c mut ChDBConnection,
}

impl<'c> ChDBTransaction<'c> {
    pub async fn new(_connection: &'c mut ChDBConnection) -> Result<Self> {
        Err(anyhow!("chDB transactions are not supported"))
    }
}

impl_executor_transaction!(ChDBDriver, ChDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for ChDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        Err(anyhow!("chDB transactions are not supported"))
    }

    async fn rollback(self) -> Result<()> {
        Err(anyhow!("chDB transactions are not supported"))
    }
}
