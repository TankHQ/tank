use redis::{Cmd, Pipeline};
use std::fmt::{self, Debug, Display};
use tank_core::{AsValue, ColumnDef, Prepared, Result, TableRef};

#[derive(Default, Debug)]
pub struct ValkeyPrepared {
    pub(crate) commands: Vec<Cmd>,
    pub(crate) table: TableRef,
    pub(crate) columns: Vec<&'static ColumnDef>,
}

impl ValkeyPrepared {
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

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>
    where
        Self: Sized,
    {
        Ok(self)
    }

    fn bind_index(&mut self, value: impl tank_core::AsValue, index: u64) -> Result<&mut Self>
    where
        Self: Sized,
    {
        Ok(self)
    }
}

impl Display for ValkeyPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ValkeyPrepared {:?}", self.commands)
    }
}
