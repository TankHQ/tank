use crate::{PostgresConnection, PostgresPrepared, PostgresSqlWriter, PostgresTransaction};
use tank_core::Driver;

/// Postgres driver.
#[derive(Default, Debug)]
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

    const NAME: &'static [&'static str] = &["postgres"];
    fn sql_writer(&self) -> PostgresSqlWriter {
        PostgresSqlWriter {}
    }
}
