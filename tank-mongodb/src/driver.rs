use crate::{MongoDBConnection, MongoDBPrepared, MongoDBSqlWriter, MongoDBTransaction};
use std::convert::AsMut;
use tank_core::{Driver, OwnedResults, OwnedRowIter, Query, RowOwned};

/// MongoDB driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct MongoDBDriver {}
impl MongoDBDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for MongoDBDriver {
    type Connection = MongoDBConnection;
    type SqlWriter = MongoDBSqlWriter;
    type Prepared = MongoDBPrepared;
    type Transaction<'c> = MongoDBTransaction<'c>;

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = OwnedRowIter
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = OwnedResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["mongodb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        Default::default()
    }
}
