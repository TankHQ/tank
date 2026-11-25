use crate::{DuckDBConnection, DuckDBPrepared, DuckDBSqlWriter, DuckDBTransaction};
use tank_core::Driver;

/// DuckDB driver.
#[derive(Default, Clone, Copy, Debug)]
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

    const NAME: &'static [&'static str] = &["duckdb"];

    fn sql_writer(&self) -> Self::SqlWriter {
        DuckDBSqlWriter::default()
    }
}
