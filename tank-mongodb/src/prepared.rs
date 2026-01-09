use std::fmt::{self, Display, Formatter};
use tank_core::{AsValue, Prepared, Result};

#[derive(Debug)]
pub struct MongoDBPrepared {
    pub(crate) index: u64,
}

impl MongoDBPrepared {
    pub(crate) fn new() -> Self {
        Self { index: 1 }
    }
}

impl Prepared for MongoDBPrepared {
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, _value: impl AsValue, index: u64) -> Result<&mut Self> {
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for MongoDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("MongoDBPrepared")
    }
}
