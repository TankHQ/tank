use crate::{SqlParam, ValueWrap};
use anyhow::anyhow;
use std::{
    any::Any,
    borrow::Cow,
    fmt::{self, Debug, Display},
    mem,
};
use tank_core::{AsValue, Prepared, Result, ValueOwned};

/// Prepared statement wrapper for Microsoft SQL Server.
#[derive(Debug)]
pub struct MSSQLPrepared {
    pub(crate) sql: String,
    pub(crate) num_params: usize,
    pub(crate) params: Vec<ValueOwned>,
    pub(crate) index: u64,
}

impl MSSQLPrepared {
    pub(crate) fn new(sql: String) -> Self {
        let num_params = Self::count_params(&sql);
        Self {
            sql,
            num_params,
            params: Vec::new(),
            index: 0,
        }
    }

    fn count_params(sql: &str) -> usize {
        let bytes = sql.as_bytes();
        let mut max_param = 0usize;
        let mut i = 0;
        while i + 2 < bytes.len() {
            if bytes[i] == b'@' && bytes[i + 1] == b'P' && bytes[i + 2].is_ascii_digit() {
                let mut n = 0usize;
                let mut j = i + 2;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    n = n.saturating_mul(10).saturating_add((bytes[j] - b'0') as usize);
                    j += 1;
                }
                if n > max_param {
                    max_param = n;
                }
                i = j;
            } else {
                i += 1;
            }
        }
        max_param
    }

    pub(crate) fn take_params(&mut self) -> Result<Vec<SqlParam>> {
        self.index = 0;
        mem::take(&mut self.params)
            .into_iter()
            .map(|v| ValueWrap(Cow::Owned(v)).try_into().map(SqlParam))
            .collect()
    }
}

impl Prepared for MSSQLPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn is_empty(&self) -> bool {
        self.sql.is_empty()
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
        let len = self.num_params;
        if self.params.is_empty() && len > 0 {
            self.params.resize_with(len, Default::default);
        }
        let target = self.params.get_mut(index as usize).ok_or_else(|| {
            anyhow!("Index {index} cannot be bound, the query has only {len} parameters")
        })?;
        *target = value.as_value().into_owned();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for MSSQLPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MSSQLPrepared: {}", self.sql)
    }
}
