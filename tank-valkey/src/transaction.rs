use anyhow::anyhow;
use redis::{Cmd, Pipeline};
use std::future;
use tank_core::{AsQuery, Driver, Executor, OwnedResults, Query, Result, Transaction};

use crate::{ValkeyConnection, ValkeyDriver};

pub struct ValkeyTransaction<'c> {
    pub(crate) connection: &'c mut ValkeyConnection,
    pub(crate) commands: Vec<Cmd>,
}

impl<'c> Executor for ValkeyTransaction<'c> {
    type Driver = ValkeyDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Results<'s, Q::Output>>> + Send
    where
        Q: AsQuery<ValkeyDriver> + Send + 's,
    {
        async move {
            let mut query = query.as_query();
            let Query::Prepared(prepared) = query.as_mut() else {
                return Err(anyhow!(
                    "Query is not the expected tank::Query::Prepared variant (Valkey/Redis driver uses prepared)",
                ));
            };
            self.commands.extend(prepared.commands.iter().cloned());
            Ok(OwnedResults::new(Vec::new()))
        }
    }
}

impl<'c> Transaction<'c> for ValkeyTransaction<'c> {
    async fn commit(self) -> Result<()> {
        let mut pipeline = Pipeline::new();
        for command in self.commands {
            pipeline.add_command(command);
        }
        pipeline
            .query_async(&mut self.connection.connection)
            .await
            .map_err(|e| anyhow!("{e:?}"))
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        future::ready(Ok(()))
    }
}
