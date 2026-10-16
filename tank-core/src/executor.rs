use crate::{
    AsEntity, AsQuery, Driver, DynQuery, Query, QueryResult, RawQuery, Result, Results, Rows,
    RowsAffected, stream::StreamExt, writer::SqlWriter,
};
use anyhow::anyhow;
use convert_case::{Case, Casing};
use std::future::{self, Future};

/// Async query execution.
///
/// Implemented by connections.
pub trait Executor: Send {
    /// Associated driver.
    type Driver: Driver;

    /// Checks if the driver supports multiple SQL statements in a single request.
    fn accepts_multiple_statements(&self) -> bool {
        true
    }

    /// Returns the driver instance associated with this executor.
    fn driver(&self) -> Self::Driver {
        Default::default()
    }

    /// Prepares a query for execution, returning a handle to the prepared statement.
    fn prepare<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        let mut query = query.as_query();
        let query = std::mem::take(query.as_mut());
        async {
            match query {
                Query::Raw(raw) => self.do_prepare(raw).await,
                Query::Prepared(..) => Ok(query),
            }
        }
    }

    /// Internal hook for implementing prepared statement support.
    fn do_prepare(
        &mut self,
        _query: RawQuery,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        future::ready(Err(anyhow!(
            "{} does not support prepare",
            self.driver().name().to_case(Case::Pascal)
        )))
    }

    /// Executes a query and returns the streamable results (rows or affected counts).
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<impl Results>> + Send;

    /// Executes a query and returns the streamable rows.
    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<impl Rows>> + Send {
        self.run(query)
    }

    /// Aggregates affected-row counts from the query execution.
    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        let result = self.run(query);
        async move {
            let mut affected = RowsAffected::default();
            let results = result.await?;
            let mut stream = std::pin::pin!(results.results());
            while let Some(item) = stream.next().await {
                if let QueryResult::Affected(a) = item? {
                    affected.extend([a]);
                }
            }
            Ok(affected)
        }
    }

    /// Efficiently inserts a collection of entities bypassing regular SQL execution when supported by the driver.
    fn append<It>(&mut self, entities: It) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_insert(&mut query, entities, false);
        self.execute(query)
    }
}

impl<S: Executor + ?Sized> Executor for &mut S {
    type Driver = S::Driver;

    fn accepts_multiple_statements(&self) -> bool {
        (**self).accepts_multiple_statements()
    }

    fn driver(&self) -> Self::Driver {
        (**self).driver()
    }

    fn prepare<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        (**self).prepare(query)
    }

    fn do_prepare(
        &mut self,
        query: RawQuery,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        (**self).do_prepare(query)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<impl Results>> + Send {
        (**self).run(query)
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<impl Rows>> + Send {
        (**self).fetch(query)
    }

    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + Send + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        (**self).execute(query)
    }

    fn append<It>(&mut self, entities: It) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        (**self).append(entities)
    }
}
