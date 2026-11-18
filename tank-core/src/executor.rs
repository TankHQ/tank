use crate::{
    AsEntity, AsQuery, Driver, Entity, MAX_BATCH_SIZE, Query, QueryResult, RawQuery, Result, Row,
    RowsAffected, Sql,
    stream::{Stream, StreamExt},
};
use anyhow::anyhow;
use convert_case::{Case, Casing};
use std::{
    cmp::max,
    future::{self, Future},
    mem,
    pin::{Pin, pin},
    task::{self, Poll},
};

/// Stream adapter returned by [`Executor::fetch`], filtering [`Executor::run`] to rows.
struct Fetched<S>(S);

impl<S, R> Stream for Fetched<S>
where
    S: Stream<Item = Result<QueryResult<R>>>,
{
    type Item = Result<R>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        loop {
            match inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(QueryResult::Row(row)))) => return Poll::Ready(Some(Ok(row))),
                Poll::Ready(Some(Ok(QueryResult::Affected(_)))) => {}
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// Async query execution. Implemented by connections.
pub trait Executor: Send {
    /// Associated driver.
    type Driver: Driver;

    /// Checks if the driver supports multiple SQL statements in a single request.
    fn accepts_multiple_statements(&self) -> bool {
        true
    }

    /// Optimal size in bytes of a single request.
    fn batch_size(&self) -> usize {
        MAX_BATCH_SIZE
    }

    /// Returns the driver instance associated with this executor.
    fn driver(&self) -> Self::Driver {
        Default::default()
    }

    /// Prepares a query for execution, returning a handle to the prepared statement.
    ///
    /// A raw query is borrowed so the caller can keep it.
    /// A prepared query is taken and returned back as it is.
    fn prepare<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        let mut query = query.as_query(&self.driver());
        async move {
            match query.as_mut() {
                Query::Raw(RawQuery(sql)) => self.do_prepare(sql).await.map(Query::Prepared),
                query => Ok(mem::take(query)),
            }
        }
    }

    /// Internal hook for implementing prepared statement support.
    fn do_prepare(
        &mut self,
        _sql: &str,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Prepared>> + Send {
        future::ready(Err(anyhow!(
            "{} does not support prepare",
            self.driver().name().to_case(Case::Pascal)
        )))
    }

    /// Executes a query and streams the results (rows or affected counts).
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send;

    /// Executes a query and streams the resulting rows, ignoring affected counts.
    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        Fetched(self.run(query))
    }

    /// Executes a query and returns the total number of affected rows.
    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        let stream = self.run(query);
        async move {
            let mut stream = pin!(stream);
            let mut affected = RowsAffected::default();
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
        self.append_default(entities)
    }
}

/// Generic batch insert through regular SQL, for drivers that don't support actual append.
#[doc(hidden)]
pub trait AppendDefault: Executor {
    fn append_default<It>(
        &mut self,
        entities: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        const DEFAULT_ROWS: usize = 1024;
        let mut entities = entities.into_iter();
        async move {
            // Reuses the allocated SQL buffer across batches.
            let mut query = Query::<Self::Driver>::default();
            let mut rows = 0;
            let mut affected = RowsAffected::default();
            let batch_size = self.batch_size();
            let driver = self.driver();
            let table = <It::Item as AsEntity>::Entity::table();
            loop {
                // Scoped so `Peekable` and `batch` never live across `.await`, keeping the future `Send`.
                {
                    let mut batch = entities.by_ref().peekable();
                    let Some(first) = batch.peek() else {
                        break;
                    };
                    let mut out = mem::take(&mut query).into_any(); // Take the buffer
                    out.clear(); // Clear the content, keep the allocated memory
                    if rows == 0 {
                        // Probe the first entity to get the actual SQL size.
                        Sql()
                            .insert_into(table)
                            .values([first.as_entity()])
                            .write_into(&driver.sql_writer(), &mut out);
                        rows = max(batch_size.checked_div(out.len()).unwrap_or(DEFAULT_ROWS), 1);
                    }
                    let batch = batch.take(rows).collect::<Vec<_>>();
                    if out.is_empty() || batch.len() > 1 {
                        out.clear();
                        Sql()
                            .insert_into(table)
                            .values(batch.iter().map(AsEntity::as_entity))
                            .write_into(&driver.sql_writer(), &mut out);
                    }
                    rows = max(
                        batch_size
                            .saturating_mul(batch.len())
                            .checked_div(out.len())
                            .unwrap_or(DEFAULT_ROWS),
                        1,
                    );
                    query = out.into(); // Give the buffer back
                }
                affected.extend([self.execute(&mut query).await?]);
            }
            Ok(affected)
        }
    }
}

impl<E: Executor + ?Sized> AppendDefault for E {}

impl<S: Executor + ?Sized> Executor for &mut S {
    type Driver = S::Driver;

    fn accepts_multiple_statements(&self) -> bool {
        (**self).accepts_multiple_statements()
    }

    fn batch_size(&self) -> usize {
        (**self).batch_size()
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
        sql: &str,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Prepared>> + Send {
        (**self).do_prepare(sql)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        (**self).run(query)
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        (**self).fetch(query)
    }

    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
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
