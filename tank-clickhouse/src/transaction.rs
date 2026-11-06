use crate::{ClickHouseConnection, ClickHouseDriver};
use anyhow::anyhow;
use tank_core::{Result, Transaction, impl_executor_transaction};

/// ClickHouse transaction wrapper.
pub struct ClickHouseTransaction<'c> {
    connection: &'c mut ClickHouseConnection,
}

impl<'c> ClickHouseTransaction<'c> {
    pub async fn new(_connection: &'c mut ClickHouseConnection) -> Result<Self> {
        Err(anyhow!("ClickHouse transactions are not supported"))
    }
}

impl_executor_transaction!(ClickHouseDriver, ClickHouseTransaction<'c>, connection);

impl<'c> Transaction<'c> for ClickHouseTransaction<'c> {
    async fn commit(self) -> Result<()> {
        Err(anyhow!("ClickHouse transactions are not supported"))
    }

    async fn rollback(self) -> Result<()> {
        Err(anyhow!("ClickHouse transactions are not supported"))
    }
}
