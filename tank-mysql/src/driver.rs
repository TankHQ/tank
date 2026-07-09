use crate::{
    MariaDBConnection, MariaDBPrepared, MariaDBSqlWriter, MariaDBTransaction, MySQLConnection,
    MySQLPrepared, MySQLSqlWriter, MySQLTransaction,
};
use tank_core::Driver;

/// MySQL driver.
#[derive(Clone, Copy, Default, Debug)]
pub struct MySQLDriver;

impl MySQLDriver {
    pub const fn new() -> Self {
        Self
    }
}

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

/// MariaDB driver.
#[derive(Clone, Copy, Default, Debug)]
pub struct MariaDBDriver;

impl MariaDBDriver {
    pub const fn new() -> Self {
        Self
    }
}

impl Driver for MariaDBDriver {
    type Connection = MariaDBConnection;
    type SqlWriter = MariaDBSqlWriter;
    type Prepared = MariaDBPrepared;
    type Transaction<'c> = MariaDBTransaction<'c>;

    const NAME: &'static [&'static str] = &["mariadb"];
    fn sql_writer(&self) -> Self::SqlWriter {
        MariaDBSqlWriter { maria_db: true }
    }
}
