use crate::{MySQLConnection, MySQLPrepared, MySQLSqlWriter, MySQLTransaction};
use tank_core::Driver;

/// MySQL/MariaDB driver.
///
/// Set `mariadb: true` (via [`MySQLDriver::mariadb()`]) for MariaDB mode,
/// which enables MariaDB-specific SQL (e.g. native `UUID` column type).
#[derive(Clone, Copy, Debug)]
pub struct MySQLDriver {
    pub(crate) mariadb: bool,
}

impl MySQLDriver {
    pub const fn new() -> Self {
        Self { mariadb: false }
    }

    /// Construct a driver configured for MariaDB.
    pub const fn mariadb() -> Self {
        Self { mariadb: true }
    }
}

impl Default for MySQLDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl Driver for MySQLDriver {
    type Connection = MySQLConnection;
    type SqlWriter = MySQLSqlWriter;
    type Prepared = MySQLPrepared;
    type Transaction<'c> = MySQLTransaction<'c>;

    const NAME: &'static [&'static str] = &["mysql", "mariadb"];

    fn sql_writer(&self) -> Self::SqlWriter {
        if self.mariadb {
            MySQLSqlWriter::mariadb()
        } else {
            MySQLSqlWriter::default()
        }
    }
}

/// MariaDB driver alias. Construct with [`MySQLDriver::mariadb()`].
pub type MariaDBDriver = MySQLDriver;
