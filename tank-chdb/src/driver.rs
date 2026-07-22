use crate::{ChdbConnection, ChdbPrepared, ChdbSqlWriter, ChdbTransaction};
use tank_core::Driver;

/// chDB driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct ChdbDriver {}

impl ChdbDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for ChdbDriver {
    type Connection = ChdbConnection;
    type SqlWriter = ChdbSqlWriter;
    type Prepared = ChdbPrepared;
    type Transaction<'c> = ChdbTransaction<'c>;

    const NAME: &'static [&'static str] = &["chdb"];

    fn sql_writer(&self) -> ChdbSqlWriter {
        ChdbSqlWriter::chdb()
    }
}
