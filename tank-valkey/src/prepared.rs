use redis::Cmd;
use std::{fmt::{self, Debug, Display}, borrow::Cow};
use tank_core::{AsValue, Error, Prepared, Result, Value, TableRef};

#[derive(Clone, Debug)]
pub enum Payload {
    Command(Cmd),
    Select(Box<SelectPayload>),
    Empty
}

#[derive(Clone, Debug)]
pub struct SelectPayload {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub key_prefix: String,
    pub key_suffix: Option<String>,
    pub exact_key: bool,
}

#[derive(Debug)]
pub struct ValkeyPrepared {
    pub(crate) payload: Payload,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

impl ValkeyPrepared {
    pub fn new() -> Self {
        Self {
            payload: Payload::Empty,
            params: Default::default(),
            index: 0,
        }
    }

    pub fn with_command(command: Cmd) -> Self {
        Self {
            payload: Payload::Command(command),
            params: Default::default(),
            index: 0,
        }
    }

    pub fn with_select(select: SelectPayload) -> Self {
        Self {
            payload: Payload::Select(Box::new(select)),
            params: Default::default(),
            index: 0,
        }
    }
}

impl Default for ValkeyPrepared {
    fn default() -> Self {
        Self::new()
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
            self.params.resize(index as usize + 1, Value::Null);
        }
        self.params[index as usize] = value.as_value();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ValkeyPrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.payload)
    }
}
