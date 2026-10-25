use crate::{ScyllaDBConnection, ScyllaDBDriver, ScyllaDBPrepared, ValueWrap};
use scylla::statement::batch::Batch;
use std::future::Future;
use tank_core::{
    AsEntity, AsQuery, Driver, DynQuery, Error, ErrorContext, Executor, OwnedResults, Query,
    RawQuery, Result, RowsAffected, SqlWriter, Transaction, truncate_long,
};

/// Transaction abstraction for ScyllaDB/Cassandra batches.
///
/// Collects batch statements and parameters and executes them on commit. Please note this is a different behavior
/// compared to traditional transactions as ScyllaDB/Cassandra do not support multi-statement transactions natively.
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

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<ScyllaDBDriver>> {
        let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
        let statement = self
            .connection
            .session
            .prepare(sql.as_str())
            .await
            .with_context(make_context)?;
        Ok(Query::Prepared(ScyllaDBPrepared::new(statement)))
    }

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Results<'s, Q::Output>>> + Send
    where
        Q: AsQuery<ScyllaDBDriver> + Send + 's,
    {
        async move {
            let mut query = query.as_query();
            let context = format!(
                "While running the query (appending a statement to a ScyllaDB/Cassandra batch):\n{:?}",
                query.as_mut()
            );
            match query.as_mut() {
                Query::Raw(RawQuery { sql, .. }) => {
                    self.params.push(Default::default());
                    self.batch.append_statement(sql.as_str());
                }
                Query::Prepared(prepared) => {
                    self.params.push(prepared.take_params().context(context)?);
                    self.batch.append_statement(prepared.statement.clone());
                }
            }
            Ok(OwnedResults::new(Vec::new()))
        }
    }

    async fn append<It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        let writer = self.driver().sql_writer();
        let mut query = DynQuery::default();
        // Use a match loop so `entity` is scoped to the `Some` arm and is provably
        // dead before the `.await`, keeping `It::Item: Send` out of the requirements.
        let mut iter = entities.into_iter();
        loop {
            match iter.next() {
                None => break,
                Some(entity) => {
                    writer.write_insert(&mut query, [entity], false);
                    // entity dropped here at end of Some arm.
                }
            }
            let mut q = query.into_query(self.driver());
            self.execute(&mut q).await?;
            query = q.into();
            query.clear();
        }
        Ok(Default::default())
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
        // Nothing to do, the batch is executed on commit in ScyllaDB/Cassandra which means nothing was sent so far
        Ok(())
    }
}
