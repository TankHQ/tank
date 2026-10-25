use crate::TankParam;
use anyhow::anyhow;
use std::fmt::{self, Debug, Display};
use tank_core::{AsValue, Prepared, Result, ValueOwned};

/// Postgres prepared statement.
#[derive(Debug)]
pub struct PostgresPrepared {
    pub(crate) sql: String,
    pub(crate) params: Vec<ValueOwned>,
    pub(crate) encoded_params: Vec<TankParam>,
    pub(crate) index: u64,
}

impl PostgresPrepared {
    pub(crate) fn new(sql: String) -> Self {
        Self {
            sql,
            params: Vec::new(),
            encoded_params: Vec::new(),
            index: 0,
        }
    }

    /// Encodes the bound values into wtx parameters.
    pub(crate) fn encode_params(&mut self) -> Result<()> {
        self.index = 0;
        let result = TankParam::encode_all(&self.params, &mut self.encoded_params);
        self.params.clear();
        result
    }
}

impl Prepared for PostgresPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let index = index as usize;
        if index > self.params.len() {
            return Err(anyhow!(
                "Index {index} cannot be bound, only {} parameters are currently bound",
                self.params.len(),
            ));
        }
        if index == self.params.len() {
            self.params.push(value.as_value().into_owned());
        } else {
            self.params[index] = value.as_value().into_owned();
        }
        self.index = index as u64 + 1;
        Ok(self)
    }
}

impl Display for PostgresPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresPrepared: {}", self.sql)
    }
}
