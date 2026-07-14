use anyhow::anyhow;
use std::{
    fmt::{self, Debug, Display},
    mem,
};
use tank_core::{AsValue, Context, DynQuery, Fragment, Prepared, Result, SqlWriter, Value};

/// ClickHouse prepared statement.
#[derive(Debug)]
pub struct ClickHousePrepared {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

impl ClickHousePrepared {
    pub(crate) fn new(sql: String) -> Self {
        Self {
            sql,
            params: Vec::new(),
            index: 0,
        }
    }

    pub(crate) fn build_sql(&self, writer: &impl SqlWriter) -> Result<String> {
        let mut out = DynQuery::default();
        let mut context = Context::fragment(Fragment::SqlSelectWhere);
        let mut param_iter = self.params.iter();
        let mut remaining = self.sql.as_str();
        while let Some(pos) = remaining.find('?') {
            out.push_str(&remaining[..pos]);
            let value = param_iter
                .next()
                .ok_or_else(|| anyhow!("Not enough parameters bound for prepared statement"))?;
            writer.write_value(&mut context, &mut out, value);
            remaining = &remaining[pos + 1..];
        }
        out.push_str(remaining);
        Ok(String::from(out))
    }

    pub(crate) fn take_params(&mut self) -> Vec<Value> {
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
        let count = self.sql.chars().filter(|&c| c == '?').count() as u64;
        if self.params.is_empty() {
            self.params.resize_with(count as _, Default::default);
        }
        let target = self.params.get_mut(index as usize).ok_or(anyhow!(
            "Index {index} cannot be bound, the query has only {count} parameters",
        ))?;
        *target = value.as_value();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ClickHousePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHousePrepared: {}", self.sql)
    }
}
