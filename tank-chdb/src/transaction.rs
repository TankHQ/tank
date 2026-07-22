use crate::{ChdbConnection, ChdbDriver};
use anyhow::anyhow;
use tank_core::{Result, Transaction, impl_executor_transaction};

/// chDB transaction wrapper.
pub struct ChdbTransaction<'c> {
    connection: &'c mut ChdbConnection,
}

impl<'c> ChdbTransaction<'c> {
    pub async fn new(connection: &'c mut ChdbConnection) -> Result<Self> {
        let _ = connection;
        Err(anyhow!("chDB transactions are not supported"))
    }
}

impl_executor_transaction!(ChdbDriver, ChdbTransaction<'c>, connection);

impl<'c> Transaction<'c> for ChdbTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        async { Err(anyhow!("chDB transactions are not supported")) }
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        async { Err(anyhow!("chDB transactions are not supported")) }
    }
}
