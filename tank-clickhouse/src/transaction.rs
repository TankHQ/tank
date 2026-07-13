use crate::{ClickHouseConnection, ClickHouseDriver};
use tank_core::{Result, Transaction, impl_executor_transaction};

/// Transaction adaptor for ClickHouse.
///
/// ClickHouse does not support transactions in standard server configurations.
/// This is a **no-op wrapper**: `BEGIN`/`COMMIT`/`ROLLBACK` are omitted and
/// all statements run directly on the underlying connection without ACID
/// guarantees.
pub struct ClickHouseTransaction<'c> {
    connection: &'c mut ClickHouseConnection,
}

impl<'c> ClickHouseTransaction<'c> {
    pub async fn new(connection: &'c mut ClickHouseConnection) -> Result<Self> {
        Ok(Self { connection })
    }
}

impl_executor_transaction!(ClickHouseDriver, ClickHouseTransaction<'c>, connection);

impl<'c> Transaction<'c> for ClickHouseTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
