use tank_core::SqlWriter;

pub struct ValkeySqlWriter {}

impl SqlWriter for ValkeySqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
}
