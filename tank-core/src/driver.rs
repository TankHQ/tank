use crate::{Connection, Prepared, Result, Transaction, writer::SqlWriter};
use std::{borrow::Cow, fmt::Debug, future::Future};

/// Backend connector and SQL dialect provider.
pub trait Driver: Default + Debug {
    /// Concrete connection.
    type Connection: Connection<Driver = Self>;
    /// SQL dialect writer.
    type SqlWriter: SqlWriter;
    /// Prepared statement handle.
    type Prepared: Prepared;
    /// Transaction type.
    type Transaction<'c>: Transaction<'c>;

    /// Human-readable backend name.
    const NAME: &'static [&'static str];

    /// Driver name (used in URLs).
    fn name(&self) -> &'static str {
        Self::NAME[0]
    }

    /// Connect to database `url`.
    ///
    /// The returned future must be awaited to obtain the connection object.
    /// Implementations may perform I/O or validation during connection.
    fn connect(&self, url: Cow<'static, str>) -> impl Future<Output = Result<Self::Connection>> {
        Self::Connection::connect(url)
    }

    /// Create a SQL writer.
    ///
    /// Returns a writer capable of rendering SQL for this driver's dialect.
    /// Writers are expected to be cheap to construct as they are usually stateless.
    fn sql_writer(&self) -> Self::SqlWriter;
}
