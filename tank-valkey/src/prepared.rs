use anyhow::anyhow;
use redis::{Cmd, Pipeline};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
};
use tank_core::{AsValue, ColumnDef, Error, Prepared, Result};

#[derive(Default, Debug)]
pub struct ValkeyPrepared {
    pub(crate) commands: Vec<Cmd>,
    pub(crate) columns: Vec<Cow<'static, ColumnDef>>,
    /// Set when the writer cannot represent the query, so execution fails
    pub(crate) error: Option<Error>,
}

impl ValkeyPrepared {
    pub const fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn make_pipeline(&self) -> Pipeline {
        let mut pipeline = Pipeline::new();
        for cmd in &self.commands {
            pipeline.add_command(cmd.clone());
        }
        pipeline
    }

    pub fn into_pipeline(self) -> Pipeline {
        let mut pipeline = Pipeline::new();
        for cmd in self.commands {
            pipeline.add_command(cmd);
        }
        pipeline
    }
}

impl Prepared for ValkeyPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn clear_bindings(&mut self) -> Result<&mut Self>
    where
        Self: Sized,
    {
        Ok(self)
    }

    fn bind(&mut self, _value: impl AsValue) -> Result<&mut Self>
    where
        Self: Sized,
    {
        Err(anyhow!(
            "Valkey/Redis inline the values into their commands, binding is not supported"
        ))
    }

    fn bind_index(&mut self, _value: impl tank_core::AsValue, _index: u64) -> Result<&mut Self>
    where
        Self: Sized,
    {
        Err(anyhow!(
            "Valkey/Redis inline the values into their commands, binding is not supported"
        ))
    }

    fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

impl Display for ValkeyPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValkeyPrepared {:?}", self.commands)
    }
}
