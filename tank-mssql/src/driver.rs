use crate::{MSSQLConnection, MSSQLPrepared, MSSQLSqlWriter, MSSQLTransaction};
use tank_core::Driver;

/// Microsoft SQL Server / Azure SQL driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct MSSQLDriver;

impl MSSQLDriver {
    pub const fn new() -> Self {
        Self
    }
}

impl Driver for MSSQLDriver {
    type Connection = MSSQLConnection;
    type SqlWriter = MSSQLSqlWriter;
    type Prepared = MSSQLPrepared;
    type Transaction<'c> = MSSQLTransaction<'c>;

    const NAME: &'static [&'static str] = &["mssql", "sqlserver"];

    fn sql_writer(&self) -> Self::SqlWriter {
        MSSQLSqlWriter::default()
    }
}
