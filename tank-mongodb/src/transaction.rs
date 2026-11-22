use crate::{MongoDBConnection, MongoDBDriver};
use tank_core::{Result, Transaction, anyhow, impl_executor_transaction};

pub struct MongoDBTransaction<'c> {
    connection: &'c mut MongoDBConnection,
    end_connection_session: bool,
}

impl<'c> MongoDBTransaction<'c> {
    pub const fn new(connection: &'c mut MongoDBConnection, end_connection_session: bool) -> Self {
        Self {
            connection,
            end_connection_session,
        }
    }
}

impl_executor_transaction!(MongoDBDriver, MongoDBTransaction<'c>, connection);

impl<'c> Transaction<'c> for MongoDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        let Some(session) = self.connection.session.as_mut() else {
            return Err(anyhow!("MongoDB transaction has no session to commit"));
        };
        session.commit_transaction().await?;
        Ok(())
    }
    async fn rollback(self) -> Result<()> {
        let Some(session) = self.connection.session.as_mut() else {
            return Err(anyhow!("MongoDB transaction has no session to roll back"));
        };
        session.abort_transaction().await?;
        Ok(())
    }
}

impl Drop for MongoDBTransaction<'_> {
    fn drop(&mut self) {
        if self.end_connection_session {
            self.connection.session = None;
        }
    }
}
