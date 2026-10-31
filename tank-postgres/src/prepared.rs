use crate::{ValueWrap, util::postgres_type_to_value};
use anyhow::anyhow;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
    mem,
};
use tank_core::{AsValue, Prepared, Result, RowLabels, ValueOwned};
use tokio_postgres::Statement;

/// Postgres prepared statement.
#[derive(Debug)]
pub struct PostgresPrepared {
    pub(crate) statement: Statement,
    pub(crate) labels: RowLabels,
    pub(crate) params: Vec<ValueOwned>,
    pub(crate) index: u64,
}

impl PostgresPrepared {
    pub(crate) fn new(statement: Statement) -> Self {
        let labels = statement
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        Self {
            statement,
            labels,
            params: Vec::new(),
            index: 0,
        }
    }

    #[inline]
    pub(crate) fn take_params(&mut self) -> Result<Vec<ValueWrap<'static>>> {
        self.index = 0;
        if self.params.is_empty() {
            return Ok(Vec::new());
        }
        let types = self.statement.params();
        mem::take(&mut self.params)
            .into_iter()
            .enumerate()
            .map(|(i, param)| {
                Ok(ValueWrap(Cow::Owned(
                    param.try_as(&postgres_type_to_value(&types[i]))?,
                )))
            })
            .collect()
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
        let len = self.statement.params().len();
        self.params.resize_with(len, Default::default);
        let target = self.params.get_mut(index as usize).ok_or_else(|| {
            anyhow!("Index {index} cannot be bound, the query has only {len} parameters")
        })?;
        *target = value.as_value().into_owned();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for PostgresPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresPrepared: ")?;
        self.statement.fmt(f)
    }
}
