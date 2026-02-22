use std::fmt::{self, Display, Formatter};
use tank_core::{AsValue, Prepared, Result};

#[derive(Debug)]
pub struct YourDBPrepared {
    pub(crate) index: u64,
}

impl YourDBPrepared {
    pub(crate) fn new() -> Self {
        Self { index: 0 }
    }
}

impl Prepared for YourDBPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        // Clear
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for YourDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("YourDBPrepared")
    }
}
