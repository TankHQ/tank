use crate::{ScyllaDBConnection, ScyllaDBPrepared, ScyllaDBSqlWriter, ScyllaDBTransaction};
use tank_core::Driver;

#[derive(Debug, Clone, Copy, Default)]
pub struct ScyllaDBDriver;
impl ScyllaDBDriver {
    pub const fn new() -> Self {
        Self
    }
}

impl Driver for ScyllaDBDriver {
    type Connection = ScyllaDBConnection;
    type SqlWriter = ScyllaDBSqlWriter;
    type Prepared = ScyllaDBPrepared;
    type Transaction<'c> = ScyllaDBTransaction<'c>;

    const NAME: &'static str = "scylladb";
    fn sql_writer(&self) -> Self::SqlWriter {
        ScyllaDBSqlWriter::default()
    }
}
