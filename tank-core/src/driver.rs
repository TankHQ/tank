use crate::{Connection, Prepared, Result, Transaction, writer::SqlWriter};
use std::{borrow::Cow, fmt::Debug, future::Future};

/// Backend connector and dialect.
pub trait Driver: Default + Debug {
    /// Concrete connection type.
    type Connection: Connection<Driver = Self>;
    /// Dialect-specific SQL writer.
    type SqlWriter: SqlWriter;
    /// Prepared statement implementation.
    type Prepared: Prepared;
    /// Transaction implementation.
    type Transaction<'c>: Transaction<'c>;

    /// Human-readable backend name.
    const NAME: &'static [&'static str];

    /// Driver name.
    fn name(&self) -> &'static str {
        Self::NAME[0]
    }

    /// Connect to database `url`.
    fn connect(&self, url: Cow<'static, str>) -> impl Future<Output = Result<Self::Connection>> {
        Self::Connection::connect(url)
    }

    /// Get a SQL writer object.
    fn sql_writer(&self) -> Self::SqlWriter;
}
