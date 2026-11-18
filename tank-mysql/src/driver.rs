use crate::{MySQLConnection, MySQLPrepared, MySQLSqlWriter, MySQLTransaction};
use tank_core::Driver;

/// MySQL/MariaDB driver.
#[derive(Clone, Copy, Debug)]
pub struct MySQLDriver {
    pub(crate) is_mariadb: bool,
}

impl MySQLDriver {
    /// Construct a driver configured for MySQL
    pub const fn mysql() -> Self {
        Self { is_mariadb: false }
    }

    /// Construct a driver configured for MariaDB
    pub const fn mariadb() -> Self {
        Self { is_mariadb: true }
    }
}

impl Default for MySQLDriver {
    fn default() -> Self {
        Self::mysql()
    }
}

impl Driver for MySQLDriver {
    type Connection = MySQLConnection;
    type SqlWriter = MySQLSqlWriter;
    type Prepared = MySQLPrepared;
    type Transaction<'c> = MySQLTransaction<'c>;

    const NAME: &'static [&'static str] = &["mysql", "mariadb"];

    fn sql_writer(&self) -> Self::SqlWriter {
        if self.is_mariadb {
            MySQLSqlWriter::mariadb()
        } else {
            MySQLSqlWriter::mysql()
        }
    }
}

/// MariaDB driver alias. Construct with [`MySQLDriver::mariadb()`].
pub type MariaDBDriver = MySQLDriver;
