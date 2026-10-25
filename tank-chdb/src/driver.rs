use crate::{
    ChDBConnection, ChDBPrepared, ChDBSqlWriter, ChDBTransaction, connection::ChDBResults,
    streaming::ChDBRows,
};
use std::convert::AsMut;
use tank_core::{Driver, Query, RowOwned};

/// chDB driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct ChDBDriver {}

impl ChDBDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for ChDBDriver {
    type Connection = ChDBConnection;
    type SqlWriter = ChDBSqlWriter;
    type Prepared = ChDBPrepared;
    type Transaction<'c> = ChDBTransaction<'c>;

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = ChDBRows
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = ChDBResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["chdb"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ChDBSqlWriter::chdb()
    }
}
