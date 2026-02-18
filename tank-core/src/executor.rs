use crate::{
    AsQuery, Driver, DynQuery, Entity, Query, QueryResult, RawQuery, Result, RowLabeled,
    RowsAffected,
    stream::{Stream, StreamExt, TryStreamExt},
    writer::SqlWriter,
};
use std::{future::Future, mem};

/// Async query execution.
///
/// Implemented by connections.
pub trait Executor: Send + Sized {
    /// Associated driver.
    type Driver: Driver;

    /// Supports multiple statements per request.
    fn accepts_multiple_statements(&self) -> bool {
        true
    }

    /// Get the driver instance.
    fn driver(&self) -> Self::Driver
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Prepare query.
    fn prepare<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        let mut query = query.as_query();
        let query = mem::take(query.as_mut());
        async {
            match query {
                Query::Raw(RawQuery(sql)) => self.do_prepare(sql).await,
                Query::Prepared(..) => Ok(query),
            }
        }
    }

    /// Actual implementation for `prepare`.
    fn do_prepare(
        &mut self,
        sql: String,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send;

    /// Execute a query, streaming `QueryResult` (rows or affected counts).
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send;

    /// Execute a query yielding `RowLabeled` from the resulting stream (filtering out `RowsAffected`).
    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<RowLabeled>> + Send {
        self.run(query).filter_map(|v| async move {
            match v {
                Ok(QueryResult::Row(v)) => Some(Ok(v)),
                Err(e) => Some(Err(e)),
                _ => None,
            }
        })
    }

    /// Execute and aggregate affected rows counter.
    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        self.run(query)
            .filter_map(|v| async move {
                match v {
                    Ok(QueryResult::Affected(v)) => Some(Ok(v)),
                    Err(e) => Some(Err(e)),
                    _ => None,
                }
            })
            .try_collect()
    }

    /// Insert many entities efficiently.
    fn append<'a, E, It>(
        &mut self,
        entities: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        E: Entity + 'a,
        It: IntoIterator<Item = &'a E> + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_insert(&mut query, entities, false);
        self.execute(query)
    }
}
