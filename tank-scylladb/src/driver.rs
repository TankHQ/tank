use crate::{ScyllaDBConnection, ScyllaDBPrepared, ScyllaDBSqlWriter, ScyllaDBTransaction};
use tank_core::Driver;

/// Driver entry for ScyllaDB/Cassandra backends.
///
/// Provides associated types for connection, prepared statements, transactions and SQL writer suitable for ScyllaDB/Cassandra usage.
/// It uses the `scylla` crate under the hood which is designed to be compatible with both ScyllaDB and Apache Cassandra databases.
#[derive(Default, Clone, Copy, Debug)]
pub struct ScyllaDBDriver {
    pub(crate) cassandra: bool,
}

impl ScyllaDBDriver {
    pub const fn new() -> Self {
        Self::scylladb()
    }

    /// Construct a driver configured for ScyllaDB.
    pub const fn scylladb() -> Self {
        Self { cassandra: false }
    }

    /// Construct a driver configured for Apache Cassandra.
    pub const fn cassandra() -> Self {
        Self { cassandra: true }
    }
}

pub type CassandraDriver = ScyllaDBDriver;

impl Driver for ScyllaDBDriver {
    type Connection = ScyllaDBConnection;
    type SqlWriter = ScyllaDBSqlWriter;
    type Prepared = ScyllaDBPrepared;
    type Transaction<'c> = ScyllaDBTransaction<'c>;

    const NAME: &'static [&'static str] = &["scylladb", "cassandra"];

    fn sql_writer(&self) -> Self::SqlWriter {
        if self.cassandra {
            ScyllaDBSqlWriter::cassandra()
        } else {
            ScyllaDBSqlWriter::scylladb()
        }
    }
}
