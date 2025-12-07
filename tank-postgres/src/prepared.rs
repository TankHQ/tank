use crate::ValueWrap;
use std::{
    fmt::{self, Debug, Display},
    mem,
};
use tank_core::{AsValue, Error, Prepared, Result};
use tokio_postgres::{Portal, Statement};

pub struct PostgresPrepared {
    pub(crate) statement: Statement,
    pub(crate) index: u64,
    pub(crate) params: Vec<ValueWrap>,
}

impl PostgresPrepared {
    pub(crate) fn new(statement: Statement) -> Self {
        Self {
            statement,
            index: 0,
            params: Vec::new(),
        }
    }
    pub(crate) fn take_params(&mut self) -> Vec<ValueWrap> {
        mem::take(&mut self.params)
    }
}

impl Prepared for PostgresPrepared {
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)?;
        Ok(self)
    }
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let len = self.statement.params().len();
        self.params.resize_with(len, Default::default);
        let target = self
            .params
            .get_mut(index as usize)
            .ok_or(Error::msg(format!(
                "Index {index} cannot be bound, the query has only {} parameters",
                len
            )))?;
        *target = value.as_value().into();
        self.index += 1;
        Ok(self)
    }
}

impl Display for PostgresPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.statement.fmt(f)
    }
}

impl Debug for PostgresPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresPrepared")
            .field("statement", &self.statement)
            .field("index", &self.index)
            .finish()
    }
}
