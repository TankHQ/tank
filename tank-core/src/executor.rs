use crate::{
    DynQuery, AsQuery, Driver, Entity, Query, QueryResult, Result, RowLabeled, RowsAffected,
    stream::{Stream, StreamExt, TryStreamExt},
    writer::SqlWriter,
};
use std::future::Future;

/// Async query executor bound to a concrete `Driver`.
///
/// Responsibilities:
/// - Translate high-level operations into driver queries
/// - Stream results without buffering the entire result set (if possible)
/// - Provide ergonomic helpers for common patterns
///
/// Implementors typically wrap a connection or pooled handle.
pub trait Executor: Send + Sized {
    /// Associated driver.
    type Driver: Driver;

    /// Returns true if the executor accepts multiple SQL statements in a single
    /// request (e.g. `CREATE; INSERT; SELECT`). Defaults to `true`.
    fn accepts_multiple_statements(&self) -> bool {
        true
    }

    /// Driver instance.
    ///
    /// Default implementation returns `Default::default()` for the associated
    /// `Driver`. Executors that carry per-connection or pooled driver state
    /// should override this method to return the appropriate driver instance.
    fn driver(&self) -> Self::Driver
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Prepare a query for later execution.
    fn prepare(
        &mut self,
        query: String,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send;

    /// Run a query, streaming `QueryResult` items.
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send;

    /// Stream only labeled rows (filters non-row results).
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

    /// Execute and aggregate affected rows.
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
