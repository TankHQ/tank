use crate::ValueWrap;
use mysql_async::Statement;
use std::{
    fmt::{self, Display},
    mem,
};
use tank_core::{AsValue, Error, Prepared, Result, Value};

#[derive(Debug)]
pub struct MySQLPrepared {
    pub(crate) statement: Statement,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

pub type MariaDBPrepared = MySQLPrepared;

impl MySQLPrepared {
    pub(crate) fn new(statement: Statement) -> Self {
        Self {
            statement,
            params: Vec::new(),
            index: 0,
        }
    }
    pub(crate) fn take_params(&mut self) -> Result<mysql_async::Params> {
        Ok(mysql_async::Params::Positional(
            mem::take(&mut self.params)
                .into_iter()
                .map(|v| ValueWrap(v).try_into())
                .collect::<Result<_>>()?,
        ))
    }
}

impl Prepared for MySQLPrepared {
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let len = self.statement.num_params();
        if self.params.is_empty() {
            self.params.resize_with(len as _, Default::default);
        }
        let target = self
            .params
            .get_mut(index as usize)
            .ok_or(Error::msg(format!(
                "Index {index} cannot be bound, the query has only {len} parameters",
            )))?;
        *target = value.as_value();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for MySQLPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MySQLPrepared")
    }
}
