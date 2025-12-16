use crate::{Connection, Prepared, Result, Transaction, writer::SqlWriter};
use std::{borrow::Cow, fmt::Debug, future::Future};

/// Backend connector and SQL dialect provider.
pub trait Driver: Debug {
    /// Concrete connection.
    type Connection: Connection;
    /// SQL dialect writer.
    type SqlWriter: SqlWriter;
    /// Prepared statement handle.
    type Prepared: Prepared;
    /// Transaction type.
    type Transaction<'c>: Transaction<'c>;

    /// Human-readable backend name.
    const NAME: &'static str;

    /// Driver name (used in URLs).
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Connect to database `url`.
    fn connect(&self, url: Cow<'static, str>) -> impl Future<Output = Result<impl Connection>> {
        Self::Connection::connect(url)
    }

    /// Create a SQL writer.
    fn sql_writer(&self) -> Self::SqlWriter;
}
