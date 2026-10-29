use crate::{
    AsEntity, AsQuery, Driver, DynQuery, Error, Query, QueryResult, RawQuery, Result, RowOwned,
    RowsAffected, writer::SqlWriter,
};
use anyhow::anyhow;
use convert_case::{Case, Casing};
use futures::FutureExt;
use std::{
    future::{self, Future},
    mem, vec,
};

/// Iterator over the rows of a materialised (owned) result set.
///
/// A named adapter so drivers that collect results up front can name their row iterator.
pub struct OwnedRowIter {
    inner: vec::IntoIter<RowOwned>,
}
impl OwnedRowIter {
    /// Wraps an owned row vector.
    pub fn new(rows: Vec<RowOwned>) -> Self {
        Self {
            inner: rows.into_iter(),
        }
    }
}

impl Iterator for OwnedRowIter {
    type Item = Result<RowOwned>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Iterator over the results of a materialised (owned) query.
///
/// A named adapter so drivers that collect results up front can name their results iterator.
pub struct OwnedResults {
    inner: vec::IntoIter<Result<QueryResult<vec::IntoIter<RowOwned>>>>,
}
impl OwnedResults {
    /// Wraps a collected vector of owned query results.
    pub fn new(results: Vec<Result<QueryResult<vec::IntoIter<RowOwned>>>>) -> Self {
        Self {
            inner: results.into_iter(),
        }
    }
}

impl Iterator for OwnedResults {
    type Item = Result<QueryResult<OwnedRowIter>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.inner.next()? {
            Ok(QueryResult::Rows(rows)) => Ok(QueryResult::Rows(OwnedRowIter { inner: rows })),
            Ok(QueryResult::Affected(affected)) => Ok(QueryResult::Affected(affected)),
            Err(error) => Err(error),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Flattened row iterator returned by [`Executor::fetch`].
///
/// Discards affected counts and yields every row set in order, propagating row and statement errors.
pub struct Fetched<'s, D, O>
where
    D: Driver + 's,
    O: AsMut<Query<D>> + Send + 's,
{
    inner: D::Results<'s, O>,
    current: Option<D::Rows<'s, O>>,
    /// Leading statement error captured while priming the first row set.
    pending: Option<Error>,
}

impl<'s, D, O> Fetched<'s, D, O>
where
    D: Driver + 's,
    O: AsMut<Query<D>> + Send + 's,
{
    /// Wraps the statement-level results produced by [`Executor::run`].
    ///
    /// Primes the first row set so [`size_hint`](Iterator::size_hint) can report
    /// the row count before iteration, letting `collect` preallocate.
    #[inline(always)]
    pub fn new(mut inner: D::Results<'s, O>) -> Self {
        for item in &mut inner {
            match item {
                Ok(QueryResult::Rows(rows)) => {
                    return Self {
                        inner,
                        current: Some(rows),
                        pending: None,
                    };
                }
                Ok(QueryResult::Affected(_)) => {}
                Err(error) => {
                    return Self {
                        inner,
                        current: None,
                        pending: Some(error),
                    };
                }
            }
        }
        Self {
            inner,
            current: None,
            pending: None,
        }
    }
}

impl<'s, D, O> Iterator for Fetched<'s, D, O>
where
    D: Driver + 's,
    O: AsMut<Query<D>> + Send + 's,
{
    type Item = Result<D::Row<'s>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        // Return `Result<Option<Row>>` instead of `Option<Result<Row>>` so `fallback` does not
        // share `next`'s `sret` slot and block LLVM SROA from scalarizing the hot row return.
        #[cold]
        #[inline(never)]
        fn fallback<'s, D, O>(fetched: &mut Fetched<'s, D, O>) -> Result<Option<D::Row<'s>>>
        where
            D: Driver + 's,
            O: AsMut<Query<D>> + Send + 's,
        {
            fetched.current = None;
            if let Some(error) = fetched.pending.take() {
                return Err(error);
            }
            for item in &mut fetched.inner {
                match item? {
                    QueryResult::Rows(mut rows) => {
                        if let Some(row) = rows.next() {
                            fetched.current = Some(rows);
                            return row.map(Some);
                        }
                    }
                    QueryResult::Affected(_) => {}
                }
            }
            Ok(None)
        }
        if let Some(row) = self.current.as_mut().and_then(Iterator::next) {
            Some(row)
        } else {
            fallback(self).transpose()
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Further statements may follow, so only the primed row set bounds the count.
        (
            match (&self.pending, &self.current) {
                (Some(_), _) => 1,
                (None, Some(rows)) => rows.size_hint().0,
                (None, None) => 0,
            },
            None,
        )
    }
}

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
        let query = mem::take(query.as_mut());
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

    /// Most general method that sends the SQL to the database and gives back rows or affected counts in the order it received.
    ///
    /// Iterator types are named on [`Self::Driver`], so drivers control how their rows borrow.
    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Results<'s, Q::Output>>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's;

    /// Filters `run` to row sets and flattens their rows.
    fn fetch<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<Fetched<'s, Self::Driver, Q::Output>>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's,
        Q: 's,
        Q::Output: 's,
    {
        self.run(query).map(|result| result.map(Fetched::new))
    }

    /// Drains `run`, propagating errors and totalling affected counts.
    fn execute<'s, Q>(&'s mut self, query: Q) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's,
        Q: 's,
        Q::Output: 's,
    {
        self.run(query).map(|result| {
            let mut affected = RowsAffected::default();
            for item in result? {
                match item? {
                    QueryResult::Affected(a) => affected.extend([a]),
                    QueryResult::Rows(rows) => {
                        for row in rows {
                            row?;
                        }
                    }
                }
            }
            Ok(affected)
        })
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

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Results<'s, Q::Output>>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's,
    {
        (**self).run(query)
    }

    fn fetch<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<Fetched<'s, Self::Driver, Q::Output>>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's,
        Q: 's,
        Q::Output: 's,
    {
        (**self).fetch(query)
    }

    fn execute<'s, Q>(&'s mut self, query: Q) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Q: AsQuery<Self::Driver> + Send + 's,
        Q: 's,
        Q::Output: 's,
    {
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
