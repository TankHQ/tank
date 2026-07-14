use anyhow::anyhow;
use crate::{ClickHouseConnection, ClickHouseDriver};
use tank_core::{Result, Transaction, impl_executor_transaction};

/// ClickHouse transaction wrapper.
pub struct ClickHouseTransaction<'c> {
    connection: &'c mut ClickHouseConnection,
}

impl<'c> ClickHouseTransaction<'c> {
    pub async fn new(connection: &'c mut ClickHouseConnection) -> Result<Self> {
        let _ = connection;
        Err(anyhow!("ClickHouse transactions are not supported"))
    }
}

impl_executor_transaction!(ClickHouseDriver, ClickHouseTransaction<'c>, connection);

impl<'c> Transaction<'c> for ClickHouseTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        async { Err(anyhow!("ClickHouse transactions are not supported")) }
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        async { Err(anyhow!("ClickHouse transactions are not supported")) }
    }
}
