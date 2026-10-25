use crate::{
    PostgresConnection, PostgresPrepared, PostgresSqlWriter, PostgresTransaction,
    rows::{PostgresResults, PostgresRow, PostgresRows},
};
use std::convert::AsMut;
use tank_core::{Driver, Query};

/// Postgres driver.
#[derive(Default, Clone, Debug)]
pub struct PostgresDriver {}
impl PostgresDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for PostgresDriver {
    type Connection = PostgresConnection;
    type SqlWriter = PostgresSqlWriter;
    type Prepared = PostgresPrepared;
    type Transaction<'c> = PostgresTransaction<'c>;

    type Row<'s> = PostgresRow<'s>;
    type Rows<'s, O>
        = PostgresRows<'s>
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = PostgresResults<'s>
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["postgres", "postgresql"];
    fn sql_writer(&self) -> PostgresSqlWriter {
        PostgresSqlWriter {}
    }
}
