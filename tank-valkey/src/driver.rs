use crate::{ValkeyConnection, ValkeyPrepared, ValkeySqlWriter, ValkeyTransaction};
use tank_core::Driver;

/// Valkey driver.
#[derive(Clone, Copy, Debug)]
pub struct ValkeyDriver {
    separator: &'static str,
    keys_with_names: bool,
}

impl ValkeyDriver {
    pub const fn new(separator: &'static str, keys_with_names: bool) -> Self {
        Self {
            separator,
            keys_with_names,
        }
    }
}

impl Default for ValkeyDriver {
    fn default() -> Self {
        Self {
            separator: ":",
            keys_with_names: false,
        }
    }
}

impl Driver for ValkeyDriver {
    type Connection = ValkeyConnection;
    type SqlWriter = ValkeySqlWriter;
    type Prepared = ValkeyPrepared;
    type Transaction<'c> = ValkeyTransaction<'c>;

    const NAME: &'static [&'static str] = &["valkey", "redis"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ValkeySqlWriter::new(self.separator, self.keys_with_names)
    }
}
