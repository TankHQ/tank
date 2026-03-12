use redis::{Cmd, Pipeline};
use std::future;
use tank_core::{
    AsQuery, Error, Executor, Query, QueryResult, Result, Transaction,
    future::Either,
    stream::{self, Stream},
};

use crate::{ValkeyConnection, ValkeyDriver};

pub struct ValkeyTransaction<'c> {
    pub(crate) connection: &'c mut ValkeyConnection,
    pub(crate) commands: Vec<Cmd>,
}

impl<'c> Executor for ValkeyTransaction<'c> {
    type Driver = ValkeyDriver;

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ValkeyDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let Query::Prepared(prepared) = query.as_mut() else {
            return Either::Left(stream::once(future::ready(Err(Error::msg(
                "Query is not the expected tank::Query::Prepared variant (Valkey/Redis driver uses prepared)",
            )))));
        };
        self.commands.extend(prepared.commands.iter().cloned());
        Either::Right(stream::empty())
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
            .map_err(|e| Error::msg(format!("{e:?}")))
    }

    fn rollback(self) -> impl Future<Output = Result<()>> {
        future::ready(Ok(()))
    }
}
