use crate::{ValkeyConnection, ValkeyPrepared, ValkeySqlWriter};
use tank_core::Driver;

/// Valkey driver.
#[derive(Default, Clone, Copy, Debug)]
pub struct ValkeyDriver {}

impl Driver for ValkeyDriver {
    type Connection = ValkeyConnection;

    type SqlWriter = ValkeySqlWriter;

    type Prepared = ValkeyPrepared;

    type Transaction<'c>;

    const NAME: &'static [&'static str] = &["valkey", "redis"];

    fn sql_writer(&self) -> Self::SqlWriter {
        todo!()
    }
}
