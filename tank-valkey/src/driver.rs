use crate::{ValkeyConnection, ValkeyPrepared, ValkeySqlWriter, ValkeyTransaction};
use tank_core::Driver;

/// Valkey driver.
#[derive(Clone, Copy, Debug)]
pub struct ValkeyDriver {
    separator: &'static str,
}

impl ValkeyDriver {
    pub const fn new(separator: &'static str) -> Self {
        Self { separator }
    }
}

impl Default for ValkeyDriver {
    fn default() -> Self {
        Self { separator: ":" }
    }
}

impl Driver for ValkeyDriver {
    type Connection = ValkeyConnection;
    type SqlWriter = ValkeySqlWriter;
    type Prepared = ValkeyPrepared;
    type Transaction<'c> = ValkeyTransaction<'c>;

    const NAME: &'static [&'static str] = &["valkey", "redis"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ValkeySqlWriter::new(self.separator)
    }
}
