use crate::{
    AsQuery, Driver, DynQuery, Entity, Error, Query, QueryResult, RawQuery, Result, Row,
    RowsAffected,
    stream::{Stream, StreamExt, TryStreamExt},
    writer::SqlWriter,
};
use convert_case::{Case, Casing};
use std::{
    future::{self, Future},
    mem,
};

/// Async query execution.
///
/// Implemented by connections.
pub trait Executor: Send + Sized {
    /// Associated driver.
    type Driver: Driver;

    /// Checks if the driver supports multiple SQL statements in a single request.
    fn accepts_multiple_statements(&self) -> bool {
        true
    }

    /// Returns the driver instance associated with this executor.
    fn driver(&self) -> Self::Driver
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Prepares a query for execution, returning a handle to the prepared statement.
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

    /// Internal hook for implementing prepared statement support.
    fn do_prepare(
        &mut self,
        _sql: String,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        future::ready(Err(Error::msg(format!(
            "{} does not support prepare",
            self.driver().name().to_case(Case::Pascal)
        ))))
    }

    /// Executes a query and streams the results (rows or affected counts).
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send;

    /// Executes a query and streams the resulting rows, ignoring affected counts.
    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<Row>> + Send {
        self.run(query).filter_map(|v| async move {
            match v {
                Ok(QueryResult::Row(v)) => Some(Ok(v)),
                Err(e) => Some(Err(e)),
                _ => None,
            }
        })
    }

    /// Executes a query and returns the total number of affected rows.
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

    /// Efficiently inserts a collection of entities bypassing regular SQL execution when supported by the driver.
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
