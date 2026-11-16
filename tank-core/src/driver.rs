use crate::{
    Connection, ConnectionPool, DBConnectionManager, Error, PoolConfig, Prepared, Result,
    Transaction, writer::SqlWriter,
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
        Self::NAME.first().copied().unwrap_or_default()
    }

    /// Creates a connection pool from a string literal, owned `String`, or `Cow` URL.
    fn connect_pool<U: Into<Cow<'static, str>>>(
        &self,
        url: U,
        config: PoolConfig,
    ) -> impl Future<Output = Result<impl ConnectionPool<Self> + use<Self, U>>> + Send {
        let url = url.into();
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
