use crate::{YourDBConnection, YourDBPrepared, YourDBSqlWriter, YourDBTransaction};
use std::convert::AsMut;
use tank_core::{Driver, OwnedResults, OwnedRowIter, Query, RowOwned};

#[derive(Default, Clone, Copy, Debug)]
pub struct YourDBDriver;
impl YourDBDriver {
    pub const fn new() -> Self {
        Self
    }
}

impl Driver for YourDBDriver {
    type Connection = YourDBConnection;
    type SqlWriter = YourDBSqlWriter;
    type Prepared = YourDBPrepared;
    type Transaction<'c> = YourDBTransaction<'c>;

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = OwnedRowIter
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = OwnedResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["yourdb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        YourDBSqlWriter::default()
    }
}
