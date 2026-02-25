use crate::{MySQLConnection, MySQLPrepared, MySQLSqlWriter, MySQLTransaction};
use tank_core::Driver;

/// MySQL / MariaDB driver.
#[derive(Clone, Copy, Default, Debug)]
pub struct MySQLDriver;
impl MySQLDriver {
    pub const fn new() -> Self {
        Self
    }
}

/// Alias for MariaDB.
pub type MariaDBDriver = MySQLDriver;

impl Driver for MySQLDriver {
    type Connection = MySQLConnection;
    type SqlWriter = MySQLSqlWriter;
    type Prepared = MySQLPrepared;
    type Transaction<'c> = MySQLTransaction<'c>;

    const NAME: &'static [&'static str] = &["mysql", "mariadb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        MySQLSqlWriter::default()
    }
}
