use crate::{ScyllaDBConnection, ScyllaDBDriver, ScyllaDBPrepared, ValueWrap};
use scylla::statement::batch::Batch;
use std::future;
use tank_core::{
    Error, ErrorContext, Executor, Query, Result, RowsAffected, Transaction,
    future::Either,
    stream::{self, Stream},
    truncate_long,
};

pub struct ScyllaDBTransaction<'c> {
    pub(crate) connection: &'c mut ScyllaDBConnection,
    pub(crate) batch: Batch,
    pub(crate) params: Vec<Vec<ValueWrap>>,
}

impl ScyllaDBTransaction<'_> {
    pub async fn execute_batch(self) -> Result<RowsAffected> {
        let result = self
            .connection
            .session
            .batch(&self.batch, self.params)
            .await
            .map_err(Error::new)?;
        result
            .result_not_rows()
            .context("Batches can contain only INSERT, UPDATE and DELETE statements")
            .map(|_| Default::default())
    }
}

impl<'c> Executor for ScyllaDBTransaction<'c> {
    type Driver = ScyllaDBDriver;

    fn driver(&self) -> &Self::Driver {
        &ScyllaDBDriver {}
    }

    async fn prepare(&mut self, sql: String) -> Result<tank_core::Query<Self::Driver>> {
        let context = format!(
            "While preparing the query:\n{}",
            truncate_long!(sql.as_str())
        );
        let statement = self
            .connection
            .session
            .prepare(sql)
            .await
            .with_context(|| context)?;
        Ok(Query::Prepared(ScyllaDBPrepared::new(statement)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl tank_core::AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<tank_core::QueryResult>> + Send {
        let mut query = query.as_query();
        let context = format!(
            "While running the query (appending a statement to a ScyllaDB/Cassandra batch):\n{:?}",
            query.as_mut()
        );
        match query.as_mut() {
            Query::Raw(sql) => self.batch.append_statement(sql.as_str()),
            Query::Prepared(prepared) => {
                self.params
                    .push(match prepared.take_params().context(context) {
                        Ok(v) => v,
                        Err(e) => {
                            return Either::Left(stream::once(future::ready(Err(e))));
                        }
                    });
                self.batch.append_statement(prepared.statement.clone())
            }
        }
        Either::Right(stream::empty())
    }
}

impl<'c> Transaction<'c> for ScyllaDBTransaction<'c> {
    async fn commit(self) -> Result<()> {
        self.connection
            .session
            .batch(&self.batch, self.params)
            .await
            .map(|_| ())
            .map_err(Error::new)
    }

    async fn rollback(self) -> Result<()> {
        Err(Error::msg("ScyllaDB does not support rollback on batches"))
    }
}
