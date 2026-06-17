use crate::{
    Connection, ConnectionPool, DBConnectionManager, Error, Prepared, Result, Transaction,
    writer::SqlWriter,
};
use deadpool::managed::Pool;
use std::{borrow::Cow, fmt::Debug, future::Future};

/// Backend connector and dialect.
pub trait Driver: Default + Clone + Sync + Send + Debug {
    /// Concrete connection type.
    type Connection: Connection<Driver = Self> + Debug;
    /// Dialect-specific SQL writer.
    type SqlWriter: SqlWriter;
    /// Prepared statement implementation.
    type Prepared: Prepared;
    /// Transaction implementation.
    type Transaction<'c>: Transaction<'c>;

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
    ) -> impl Future<Output = Result<impl ConnectionPool<Self>>> + Send {
        async {
            Ok(Pool::builder(DBConnectionManager::new(self.clone(), url))
                .build()
                .map_err(Error::new)?)
        }
    }

    /// Returns a dialect-specific SQL writer for query construction.
    fn sql_writer(&self) -> Self::SqlWriter;
}
