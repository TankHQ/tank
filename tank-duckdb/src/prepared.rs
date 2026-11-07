use crate::{ValueWrap, cbox::CBox};
use anyhow::anyhow;
use libduckdb_sys::v2::*;
use std::{
    borrow::Cow,
    fmt::{self, Display},
    mem,
};
use tank_core::{AsValue, ErrorContext, Prepared, Result, ValueOwned};

/// Prepared statement wrapper for DuckDB.
///
/// Manages the native `duckdb_v2_prepared_statement_handle` and bound parameter values for use with the `Prepared` trait.
#[derive(Debug)]
pub struct DuckDBPrepared {
    pub(crate) statement: CBox<duckdb_v2_prepared_statement_handle>,
    pub(crate) params: Vec<ValueOwned>,
    pub(crate) param_count: u64,
    pub(crate) index: u64,
    pub(crate) sql: String,
}

impl DuckDBPrepared {
    pub(crate) fn new(
        statement: CBox<duckdb_v2_prepared_statement_handle>,
        param_count: u64,
        sql: String,
    ) -> Self {
        Self {
            statement,
            params: Vec::new(),
            param_count,
            index: 0,
            sql,
        }
    }

    #[inline(always)]
    pub(crate) fn statement(&self) -> duckdb_v2_prepared_statement_handle {
        *self.statement
    }

    pub(crate) fn take_params(
        &mut self,
        factory: duckdb_v2_factory_handle,
    ) -> Result<Vec<CBox<duckdb_v2_value_handle>>> {
        self.index = 0;
        let len = self.param_count as usize;
        if self.params.len() < len {
            return Err(anyhow!(
                "Expected {len} bound parameters, got {}",
                self.params.len()
            ));
        }
        mem::take(&mut self.params)
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                ValueWrap(Cow::Owned(v))
                    .to_duckdb_value(factory)
                    .with_context(|| format!("While trying to bind the parameter {}", i + 1))
            })
            .collect()
    }
}

impl Display for DuckDBPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.sql)
    }
}

impl Prepared for DuckDBPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn is_empty(&self) -> bool {
        self.statement().is_null()
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        let len = self.param_count as usize;
        if self.params.is_empty() {
            self.params.resize_with(len, Default::default);
        }
        let target = self.params.get_mut(index as usize).ok_or_else(|| {
            let error = anyhow!("Index {index} cannot be bound, the query has only {len} parameters");
            log::error!("{error:#}");
            error
        })?;
        let value = v.as_value().into_owned();
        if matches!(value, ValueOwned::Unknown(..)) {
            let error = anyhow!("Cannot use a {value:?} as a query parameter");
            log::error!("{error:#}");
            return Err(error);
        }
        *target = value;
        self.index = index + 1;
        Ok(self)
    }
}
