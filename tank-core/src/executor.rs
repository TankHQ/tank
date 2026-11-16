use crate::{
    AsEntity, AsQuery, Driver, Entity, Query, QueryResult, RawQuery, Result, Row, RowsAffected,
    Sql,
    stream::{Stream, StreamExt},
    writer::SqlWriter,
};
use anyhow::anyhow;
use convert_case::{Case, Casing};
use std::{
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
        let mut query = query.as_query();
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

/// Generic batch insert through regular SQL, for drivers that don't support actual append
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
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        const BATCH_SIZE: usize = 256;
        let writer = self.driver().sql_writer();
        let mut entities = entities.into_iter();
        async move {
            let mut affected = RowsAffected::default();
            // The SQL string buffer is reused through `query`.
            let mut query = Query::<Self::Driver>::default();
            loop {
                // Scoped so the batch never lives across an await
                // which keeps the future `Send` without requiring `It::Item: Send`
                {
                    let batch = entities.by_ref().take(BATCH_SIZE).collect::<Vec<_>>();
                    if batch.is_empty() {
                        break;
                    }
                    let mut out = mem::take(&mut query).into_any();
                    out.clear(); // Clears the content, doesn't lose the allocated buffer.
                    writer.write_insert(
                        &mut out,
                        Sql()
                            .insert_into(E::<It>::table())
                            .values(batch.iter().map(AsEntity::as_entity)),
                    );
                    query = out.into(); // Give the buffer back
                };
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
