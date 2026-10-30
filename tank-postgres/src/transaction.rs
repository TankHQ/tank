use crate::{PostgresConnection, PostgresDriver};
use std::future::Future;
use tank_core::{
    AsQuery, Error, Executor, Query, QueryResult, RawQuery, Result, Row, Transaction,
    stream::Stream,
};
use wtx::database::DbClient as _;

/// Postgres transaction.
///
/// `wtx` has no dedicated transaction object, so this holds a mutable borrow of
/// the connection and issues `BEGIN`/`COMMIT`/`ROLLBACK` explicitly.
pub struct PostgresTransaction<'c> {
    pub(crate) connection: &'c mut PostgresConnection,
}

impl<'c> PostgresTransaction<'c> {
    pub(crate) async fn new(connection: &'c mut PostgresConnection) -> Result<Self> {
        connection
            .client
            .execute_ignored("BEGIN")
            .await
            .map_err(Error::new)?;
        Ok(Self { connection })
    }
}

impl<'c> Executor for PostgresTransaction<'c> {
    type Driver = PostgresDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    fn do_prepare(
        &mut self,
        raw: RawQuery,
    ) -> impl Future<Output = Result<Query<PostgresDriver>>> + Send {
        self.connection.do_prepare(raw)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        self.connection.run(query)
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        self.connection.fetch(query)
    }
}

impl<'c> Transaction<'c> for PostgresTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        async move {
            self.connection
                .client
                .execute_ignored("COMMIT")
                .await
                .map_err(Error::new)
        }
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        async move {
            self.connection
                .client
                .execute_ignored("ROLLBACK")
                .await
                .map_err(Error::new)
        }
    }
}
