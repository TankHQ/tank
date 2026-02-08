use crate::{MongoDBConnection, MongoDBDriver};
use tank_core::{Result, Transaction, impl_executor_transaction};

pub struct MongoDBTransaction<'c> {
    connection: &'c mut MongoDBConnection,
    end_connection_session: bool,
}

impl<'c> MongoDBTransaction<'c> {
    pub fn new(connection: &'c mut MongoDBConnection, end_connection_session: bool) -> Self {
        Self {
            connection,
            end_connection_session,
        }
    }
}

impl_executor_transaction!(MongoDBDriver, MongoDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for MongoDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        self.connection
            .session
            .as_mut()
            .unwrap()
            .commit_transaction()
            .await?;
        Ok(())
    }
    async fn rollback(self) -> Result<()> {
        self.connection
            .session
            .as_mut()
            .unwrap()
            .abort_transaction()
            .await?;
        Ok(())
    }
}

impl<'c> Drop for MongoDBTransaction<'c> {
    fn drop(&mut self) {
        if self.end_connection_session {
            self.connection.session = None;
        }
    }
}
