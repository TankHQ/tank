use crate::{
    Connection, ConnectionPool, DBConnectionManager, Error, PoolConfig, Prepared, Query,
    QueryResult, Result, Row, Transaction, writer::SqlWriter,
};
use deadpool::managed::Pool;
use std::{borrow::Cow, fmt::Debug, future::Future};

/// Backend connector and dialect.
///
/// Names the row shape and result iterators so `Executor::run`/`fetch` return
/// short concrete types instead of anonymous `impl Iterator` pyramids.
pub trait Driver: Default + Clone + Sync + Send + Debug {
    /// Concrete connection type.
    type Connection: Connection<Driver = Self> + Debug;
    /// Dialect-specific SQL writer.
    type SqlWriter: SqlWriter;
    /// Prepared statement implementation.
    type Prepared: Prepared;
    /// Transaction implementation.
    type Transaction<'c>: Transaction<'c>;

    /// The row type produced by this backend, borrowing or owning its values.
    type Row<'s>: Row + 's
    where
        Self: 's;

    /// Iterator over the rows of a single statement.
    ///
    /// `O` is the query's [`AsQuery`](crate::AsQuery) output, kept alive by streaming drivers.
    type Rows<'s, O>: Iterator<Item = Result<Self::Row<'s>>> + Send + 's
    where
        Self: 's,
        O: AsMut<Query<Self>> + Send + 's;

    /// Iterator over a query's results (row sets or affected counts).
    type Results<'s, O>: Iterator<Item = Result<QueryResult<Self::Rows<'s, O>>>> + Send + 's
    where
        Self: 's,
        O: AsMut<Query<Self>> + Send + 's;

    /// Human-readable backend name.
    const NAME: &'static [&'static str];

    /// Returns the primary name of the driver.
    fn name(&self) -> &'static str {
        Self::NAME[0]
    }

    /// Creates a new connection to the database at the specified URL.
    fn connect_pool(
        &self,
        url: Cow<'static, str>,
        config: PoolConfig,
    ) -> impl Future<Output = Result<impl ConnectionPool<Self>>> + Send {
        let config = config.into();
        async move {
            Ok(Pool::builder(DBConnectionManager::new(self.clone(), url))
                .config(config)
                .build()
                .map_err(Error::new)?)
        }
    }

    /// Returns a dialect-specific SQL writer for query construction.
    fn sql_writer(&self) -> Self::SqlWriter;
}
