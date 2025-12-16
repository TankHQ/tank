use crate::{
    DuckDBPrepared, DuckDBTransaction, connection::DuckDBConnection, sql_writer::DuckDBSqlWriter,
};
use tank_core::Driver;

/// DuckDB driver.
#[derive(Debug, Default, Clone, Copy)]
pub struct DuckDBDriver {}

impl DuckDBDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for DuckDBDriver {
    type Connection = DuckDBConnection;
    type SqlWriter = DuckDBSqlWriter;
    type Prepared = DuckDBPrepared;
    type Transaction<'c> = DuckDBTransaction<'c>;

    const NAME: &'static str = "duckdb";
    fn sql_writer(&self) -> Self::SqlWriter {
        DuckDBSqlWriter::default()
    }
}
