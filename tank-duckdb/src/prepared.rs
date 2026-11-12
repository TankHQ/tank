use crate::{ValueWrap, ensure_rc, util::CBox};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    fmt::{self, Display},
    mem,
};
use tank_core::{AsValue, Error, Prepared, Result, Value, error_message_from_ptr};

/// Prepared statement wrapper for DuckDB.
#[derive(Debug)]
pub struct DuckDBPrepared {
    pub(crate) statement: CBox<duckdb_prepared_statement>,
    pub(crate) index: u64,
}

impl DuckDBPrepared {
    pub(crate) fn new(statement: CBox<duckdb_prepared_statement>) -> Self {
        Self {
            statement,
            index: 0,
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
        ensure_rc!(
            unsafe { duckdb_clear_bindings(*self.statement) },
            anyhow!("Could not clear the bindings from DuckDB statement")
        );
        Ok(self)
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    #[inline(always)]
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        // DuckDB's parameter index is 1-based.
        let index = index + 1;
        unsafe {
            let prepared = *self.statement;
            let mut value = value.as_value();
            let state = match &mut value {
                Value::Boolean(Some(v)) => Ok(duckdb_bind_boolean(prepared, index, *v)),
                Value::Int32(Some(v)) => Ok(duckdb_bind_int32(prepared, index, *v)),
                Value::Int64(Some(v)) => Ok(duckdb_bind_int64(prepared, index, *v)),
                Value::Float64(Some(v)) => Ok(duckdb_bind_double(prepared, index, *v)),
                Value::Varchar(Some(v)) => {
                    let v = mem::take(v);
                    Ok(duckdb_bind_varchar_length(
                        prepared,
                        index,
                        v.as_ptr() as _,
                        v.len() as _,
                    ))
                }
                value => CBox::try_from(ValueWrap::from(mem::take(value)))
                    .map(|v| duckdb_bind_value(prepared, index, *v)),
            };
            mem::forget(value);
            ensure_rc!(
                state?,
                Error::msg(error_message_from_ptr(duckdb_prepare_error(prepared)).into_owned(),)
                    .context(format!("While trying to bind the parameter {index}"))
            );
            self.index = index;
            Ok(self)
        }
    }
}
