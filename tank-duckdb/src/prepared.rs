use crate::{ValueWrap, cbox::CBox, streaming::RowSchema};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::fmt::{self, Display};
use tank_core::{AsValue, Error, Prepared, Result, Value, error_message_from_ptr};

/// Prepared statement wrapper for DuckDB.
#[derive(Debug)]
pub struct DuckDBPrepared {
    pub(crate) statement: CBox<duckdb_prepared_statement>,
    pub(crate) index: u64,
    pub(crate) schema: Option<RowSchema>,
}

impl DuckDBPrepared {
    pub(crate) fn new(statement: CBox<duckdb_prepared_statement>) -> Self {
        Self {
            statement,
            index: 0,
            schema: None,
        }
    }
}

impl Display for DuckDBPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<prepared statement at {:p}, SQL not retrievable from DuckDB C API>",
            *self.statement
        )
    }
}

impl Prepared for DuckDBPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn is_empty(&self) -> bool {
        self.statement.is_null()
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        if self.index == 0 {
            return Ok(self);
        }
        self.index = 0;
        unsafe {
            if duckdb_clear_bindings(*self.statement) != duckdb_state_DuckDBSuccess {
                let error = anyhow!("Could not clear the bindings from DuckDB statement");
                log::error!("{error:#}");
                return Err(error);
            }
        }
        Ok(self)
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    #[inline(always)]
    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        // DuckDB's parameter index is 1-based.
        let index = index + 1;
        unsafe {
            let prepared = *self.statement;
            let state = match v.as_value() {
                Value::Boolean(Some(v)) => duckdb_bind_boolean(prepared, index, v),
                Value::Int32(Some(v)) => duckdb_bind_int32(prepared, index, v),
                Value::Int64(Some(v)) => duckdb_bind_int64(prepared, index, v),
                Value::Float64(Some(v)) => duckdb_bind_double(prepared, index, v),
                Value::Varchar(Some(v)) => {
                    duckdb_bind_varchar_length(prepared, index, v.as_ptr() as _, v.len() as _)
                }
                other => {
                    if matches!(
                        other,
                        Value::Array(Some(..), ..)
                            | Value::List(Some(..), ..)
                            | Value::Map(Some(..), ..)
                            | Value::Struct(Some(..), ..)
                    ) {
                        let error = anyhow!("Cannot use a {other:?} as a query parameter");
                        log::error!("{error:#}");
                        return Err(error);
                    }
                    duckdb_bind_value(
                        prepared,
                        index,
                        *CBox::try_from(ValueWrap::from(other))?,
                    )
                }
            };
            if state != duckdb_state_DuckDBSuccess {
                let error =
                    Error::msg(error_message_from_ptr(&duckdb_prepare_error(prepared)).into_owned())
                        .context(format!("While trying to bind the parameter {index}"));
                log::error!("{error:#}");
                return Err(error);
            }
            self.index = index;
            Ok(self)
        }
    }
}
