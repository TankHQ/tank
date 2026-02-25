use redis::Cmd;
use std::fmt::{self, Debug, Display};
use tank_core::{AsValue, Error, Prepared, Result, Value};

#[derive(Debug)]
pub struct ValkeyPrepared {
    command: Cmd,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

impl ValkeyPrepared {
    pub fn new(command: Cmd) -> Self {
        Self {
            command,
            params: Default::default(),
            index: 0,
        }
    }
    pub fn get_command(&self) -> &Cmd {
        &self.command
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
        self.params.clear();
        self.index = 0;
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>
    where
        Self: Sized,
    {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, value: impl tank_core::AsValue, index: u64) -> Result<&mut Self>
    where
        Self: Sized,
    {
        if self.params.len() <= index as _ {
            self.params.resize_with((index + 1) as _, Default::default);
        }
        let target = self
            .params
            .get_mut(index as usize)
            .ok_or(Error::msg(format!(
                "Index {index} it out of bounds for parameters",
            )))?;
        *target = value.as_value();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ValkeyPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ValkeyPrepared: ")?;
        self.command.fmt(f)
    }
}
