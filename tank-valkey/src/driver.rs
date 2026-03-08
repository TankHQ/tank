use crate::{ValkeyConnection, ValkeyPrepared, ValkeySqlWriter, ValkeyTransaction};
use tank_core::Driver;

/// Valkey driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct ValkeyDriver {}
impl ValkeyDriver {
    pub const fn new() -> Self {
        Self {}
    }
}

impl Driver for ValkeyDriver {
    type Connection = ValkeyConnection;

    type SqlWriter = ValkeySqlWriter;

    type Prepared = ValkeyPrepared;

    type Transaction<'c> = ValkeyTransaction<'c>;

    const NAME: &'static [&'static str] = &["valkey", "redis"];

    fn sql_writer(&self) -> Self::SqlWriter {
        ValkeySqlWriter {}
    }
}
