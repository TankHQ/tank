use crate::{ChDBConnection, ChDBPrepared, ChDBSqlWriter, ChDBTransaction};
use tank_core::Driver;

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

    const NAME: &'static [&'static str] = &["chdb"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ChDBSqlWriter::chdb()
    }
}
