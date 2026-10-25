use crate::{
    DuckDBPrepared, DuckDBTransaction, connection::DuckDBConnection, sql_writer::DuckDBSqlWriter,
    streaming,
};
use std::convert::AsMut;
use tank_core::{Driver, Query, RowOwned};

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

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = streaming::Rows<'s, O>
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = streaming::Statements<'s, O>
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["duckdb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        DuckDBSqlWriter::default()
    }
}
