use crate::{ScyllaDBConnection, ScyllaDBPrepared, ScyllaDBSqlWriter, ScyllaDBTransaction};
use std::convert::AsMut;
use tank_core::{Driver, OwnedResults, OwnedRowIter, Query, RowOwned};

/// Driver entry for ScyllaDB/Cassandra backends.
///
/// Provides associated types for connection, prepared statements, transactions and SQL writer suitable for ScyllaDB/Cassandra usage.
/// It uses the `scylla` crate under the hood which is designed to be compatible with both ScyllaDB and Apache Cassandra databases.
#[derive(Default, Clone, Copy, Debug)]
pub struct ScyllaDBDriver;
impl ScyllaDBDriver {
    pub const fn new() -> Self {
        Self
    }
}

pub type CassandraDriver = ScyllaDBDriver;

impl Driver for ScyllaDBDriver {
    type Connection = ScyllaDBConnection;
    type SqlWriter = ScyllaDBSqlWriter;
    type Prepared = ScyllaDBPrepared;
    type Transaction<'c> = ScyllaDBTransaction<'c>;

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = OwnedRowIter
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = OwnedResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["scylladb", "cassandra"];
    fn sql_writer(&self) -> Self::SqlWriter {
        ScyllaDBSqlWriter::default()
    }
}
