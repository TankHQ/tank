use crate::{YourDBConnection, YourDBPrepared, YourDBSqlWriter, YourDBTransaction};
use tank_core::Driver;

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

    const NAME: &'static [&'static str] = &["yourdb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        YourDBSqlWriter::default()
    }
}
