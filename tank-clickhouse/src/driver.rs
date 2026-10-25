use crate::{ClickHouseConnection, ClickHousePrepared, ClickHouseSqlWriter, ClickHouseTransaction};
use std::convert::AsMut;
use tank_core::{Driver, OwnedResults, OwnedRowIter, Query, RowOwned};

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

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = OwnedRowIter
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = OwnedResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["clickhouse"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ClickHouseSqlWriter::new()
    }
}
