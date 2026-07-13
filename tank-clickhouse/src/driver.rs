use crate::{ClickHouseConnection, ClickHousePrepared, ClickHouseSqlWriter, ClickHouseTransaction};
use tank_core::Driver;

/// ClickHouse driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct ClickHouseDriver {}
impl ClickHouseDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for ClickHouseDriver {
    type Connection = ClickHouseConnection;
    type SqlWriter = ClickHouseSqlWriter;
    type Prepared = ClickHousePrepared;
    type Transaction<'c> = ClickHouseTransaction<'c>;

    const NAME: &'static [&'static str] = &["clickhouse"];

    fn sql_writer(&self) -> ClickHouseSqlWriter {
        ClickHouseSqlWriter::new()
    }
}
