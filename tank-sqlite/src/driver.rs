use crate::{
    SQLiteConnection, SQLitePrepared, SQLiteTransaction,
    connection::{SQLiteResults, SQLiteRows},
    sql_writer::SQLiteSqlWriter,
};
use std::convert::AsMut;
use tank_core::{Driver, Query, RowOwned};

/// SQLite driver.
#[derive(Default, Clone, Debug)]
pub struct SQLiteDriver {}
impl SQLiteDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for SQLiteDriver {
    type Connection = SQLiteConnection;
    type SqlWriter = SQLiteSqlWriter;
    type Prepared = SQLitePrepared;
    type Transaction<'c> = SQLiteTransaction<'c>;

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = SQLiteRows<'s, O>
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = SQLiteResults<'s, O>
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["sqlite"];
    fn sql_writer(&self) -> SQLiteSqlWriter {
        SQLiteSqlWriter {}
    }
}
