use crate::{ValkeyConnection, ValkeyPrepared, ValkeySqlWriter, ValkeyTransaction};
use std::convert::AsMut;
use tank_core::{Driver, OwnedResults, OwnedRowIter, Query, RowOwned};

/// Valkey driver.
#[derive(Clone, Copy, Debug)]
pub struct ValkeyDriver {
    separator: &'static str,
    keys_with_names: bool,
}

pub type RedisDriver = ValkeyDriver;

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

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = OwnedRowIter
    where
        O: AsMut<Query<Self>> + Send + 's;
    type Results<'s, O>
        = OwnedResults
    where
        O: AsMut<Query<Self>> + Send + 's;

    const NAME: &'static [&'static str] = &["valkey", "redis", "valkeys", "rediss"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ValkeySqlWriter::new(self.separator, self.keys_with_names)
    }
}
