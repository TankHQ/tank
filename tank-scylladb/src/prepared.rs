use scylla::statement::prepared::PreparedStatement;
use std::{
    fmt::{self, Debug, Display, Formatter},
    mem,
};
use tank_core::{AsValue, Error, Prepared, QueryMetadata, Result};

use crate::ValueWrap;

/// Prepared statement wrapper for ScyllaDB.
///
/// Contains the `PreparedStatement`, accumulated params and current bind index used when converting `tank_core::Value` into driver parameters.
pub struct ScyllaDBPrepared {
    pub(crate) statement: PreparedStatement,
    pub(crate) params: Vec<ValueWrap>,
    pub(crate) index: u64,
    pub(crate) metadata: QueryMetadata,
}

impl ScyllaDBPrepared {
    pub(crate) fn new(statement: PreparedStatement) -> Self {
        Self {
            statement,
            params: Vec::new(),
            index: 0,
            metadata: Default::default(),
        }
    }
    pub(crate) fn take_params(&mut self) -> Result<Vec<ValueWrap>> {
        self.index = 0;
        Ok(mem::take(&mut self.params))
    }
}

impl Prepared for ScyllaDBPrepared {
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let len = self.statement.get_variable_col_specs().len();
        if self.params.is_empty() {
            self.params.resize_with(len, Default::default);
        }
        let target = self
            .params
            .get_mut(index as usize)
            .ok_or(Error::msg(format!(
                "Index {index} cannot be bound, the query has only {len} parameters",
            )))?;
        *target = value.as_value().into();
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

impl Display for ScyllaDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("ScyllaDBPrepared")
    }
}

impl Debug for ScyllaDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaDBPrepared")
            .field("index", &self.index)
            .finish()
    }
}
