use anyhow::anyhow;
use std::{
    fmt::{self, Display},
    mem,
};
use tank_core::{AsValue, Prepared, QueryParam, Result, ValueOwned};

/// ClickHouse prepared statement.
///
/// ClickHouse has no wire-level query parameter binding.
/// The equivalent is typed query parameters (`{name:Type}`) fed through `SET param_name = ...`.
/// The writer emits provisional `{pN:Nullable(String)}` markers and records their offsets.
/// [`ClickHouseSqlWriter::build_parameterized_sql`] rewrites them into the final query.
#[derive(Debug)]
pub struct ClickHousePrepared {
    pub(crate) sql: String,
    pub(crate) markers: Vec<QueryParam>,
    pub(crate) params: Vec<ValueOwned>,
    pub(crate) index: u64,
}

impl ClickHousePrepared {
    /// Create a prepared statement for the given SQL and parameter markers.
    pub fn new(sql: String, markers: Vec<QueryParam>) -> Self {
        Self {
            sql,
            markers,
            params: Vec::new(),
            index: 0,
        }
    }

    /// The raw SQL with provisional parameter markers.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The byte spans of the provisional parameter markers.
    pub fn markers(&self) -> &[QueryParam] {
        &self.markers
    }

    /// The currently bound values.
    pub fn params(&self) -> &[ValueOwned] {
        &self.params
    }

    /// Take the bound parameters, resetting the bind index.
    pub fn take_params(&mut self) -> Vec<ValueOwned> {
        self.index = 0;
        mem::take(&mut self.params)
    }
}

impl Prepared for ClickHousePrepared {
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
        let count = self.markers.len() as u64;
        if self.params.is_empty() {
            self.params.resize_with(count as _, Default::default);
        }
        let target = self.params.get_mut(index as usize).ok_or_else(|| {
            let error =
                anyhow!("Index {index} cannot be bound, the query has only {count} parameters");
            log::error!("{error:#}");
            error
        })?;
        *target = value.as_value().into_owned();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ClickHousePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHousePrepared: {}", self.sql)
    }
}
