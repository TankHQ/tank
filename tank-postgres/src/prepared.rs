use crate::ValueWrap;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
    mem,
};
use tank_core::{AsValue, Error, Prepared, QueryMetadata, Result, Value};
use tokio_postgres::Statement;

/// Prepared statement wrapper for Postgres.
///
/// Holds the `tokio_postgres::Statement` and collected parameter `Value`s for binding/execution through the `Executor` APIs.
#[derive(Debug)]
pub struct PostgresPrepared {
    pub(crate) statement: Statement,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
    pub(crate) metadata: QueryMetadata,
}

impl PostgresPrepared {
    pub(crate) fn new(statement: Statement) -> Self {
        Self {
            statement,
            params: Vec::new(),
            index: 0,
            metadata: Default::default(),
        }
    }
    pub(crate) fn take_params(&mut self) -> Vec<ValueWrap<'static>> {
        self.index = 0;
        mem::take(&mut self.params)
            .into_iter()
            .map(|v| ValueWrap(Cow::Owned(v)))
            .collect()
    }
}

impl Prepared for PostgresPrepared {
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let len = self.statement.params().len();
        self.params.resize_with(len, Default::default);
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

    fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut QueryMetadata {
        &mut self.metadata
    }
}

impl Display for PostgresPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresPrepared: ")?;
        self.statement.fmt(f)
    }
}
