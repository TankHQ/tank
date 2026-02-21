use crate::{MongoDBConnection, MongoDBPrepared, MongoDBSqlWriter, MongoDBTransaction};
use tank_core::Driver;

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

    const NAME: &'static [&'static str] = &["mongodb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        Default::default()
    }
}
