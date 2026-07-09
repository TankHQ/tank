use crate::{MariaDBConnection, MariaDBDriver, MySQLConnection, MySQLDriver, MySQLQueryable};
use mysql_async::TxOpts;
use tank_core::{Result, Transaction, impl_executor_transaction};

/// Transaction adaptor for MySQL/MariaDB.
///
/// Wraps a `mysql_async::Transaction` and implements the `Transaction`/`Executor`
/// behavior expected by the `tank_core` abstractions.
pub struct MySQLTransaction<'c> {
    pub(crate) transaction: MySQLQueryable<mysql_async::Transaction<'c>>,
}

pub struct MariaDBTransaction<'c> {
    pub(crate) transaction: MySQLQueryable<mysql_async::Transaction<'c>>,
}

impl<'c> MySQLTransaction<'c> {
    pub async fn new(connection: &'c mut MySQLConnection) -> Result<Self> {
        Ok(Self {
            transaction: MySQLQueryable::new(
                connection
                    .conn
                    .executor
                    .start_transaction(TxOpts::new())
                    .await
                    .map_err(|e| {
                        log::error!("{:#}", e);
                        e
                    })?,
            ),
        })
    }
}

impl<'c> MariaDBTransaction<'c> {
    pub async fn new(connection: &'c mut MariaDBConnection) -> Result<Self> {
        Ok(Self {
            transaction: MySQLQueryable::new(
                connection
                    .conn
                    .executor
                    .start_transaction(TxOpts::new())
                    .await
                    .map_err(|e| {
                        log::error!("{:#}", e);
                        e
                    })?,
            ),
        })
    }
}

impl_executor_transaction!(MySQLDriver, MySQLTransaction<'c>, transaction);
impl_executor_transaction!(MariaDBDriver, MariaDBTransaction<'c>, transaction);

impl<'c> Transaction<'c> for MySQLTransaction<'c> {
    async fn commit(self) -> Result<()> {
        self.transaction
            .executor
            .commit()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .executor
            .rollback()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}

impl<'c> Transaction<'c> for MariaDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        self.transaction
            .executor
            .commit()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn rollback(self) -> Result<()> {
        self.transaction
            .executor
            .rollback()
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}
