use crate::{MySQLConnection, MySQLPrepared, MySQLSqlWriter, MySQLTransaction};
use tank_core::Driver;

#[derive(Debug, Clone, Copy, Default)]
pub struct MySQLDriver;
impl MySQLDriver {
    pub const fn new() -> Self {
        Self
    }
}

pub type MariaDBDriver = MySQLDriver;

impl Driver for MySQLDriver {
    type Connection = MySQLConnection;
    type SqlWriter = MySQLSqlWriter;
    type Prepared = MySQLPrepared;
    type Transaction<'c> = MySQLTransaction<'c>;

    const NAME: &'static str = "mysql";
    fn sql_writer(&self) -> Self::SqlWriter {
        MySQLSqlWriter::default()
    }
}
