use crate::{
    cbox::CBox, date_to_duckdb_date, decimal_to_duckdb_decimal, i128_to_duckdb_hugeint,
    interval_to_duckdb_interval, offsetdatetime_to_duckdb_timestamp,
    primitive_date_time_to_duckdb_timestamp, time_to_duckdb_time, u128_to_duckdb_uhugeint,
};
use libduckdb_sys::*;
use std::{
    ffi::c_void,
    fmt::{self, Display},
};
use tank_core::{AsValue, Error, Prepared, Result, Value, error_message_from_ptr};

/// Prepared statement wrapper for DuckDB.
///
/// Manages the native `duckdb_prepared_statement` pointer and binding index for use with the `Prepared` trait.
#[derive(Debug)]
pub struct DuckDBPrepared {
    pub(crate) statement: CBox<duckdb_prepared_statement>,
    pub(crate) index: u64,
}
impl DuckDBPrepared {
    pub(crate) fn new(statement: CBox<duckdb_prepared_statement>) -> Self {
        Self {
            statement: statement.into(),
            index: 1,
        }
    }
    pub(crate) fn statement(&self) -> duckdb_prepared_statement {
        *self.statement
    }
}

impl Display for DuckDBPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.statement())
    }
}

impl Prepared for DuckDBPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.index = 1;
        unsafe {
            let rc = duckdb_clear_bindings(self.statement());
            if rc != duckdb_state_DuckDBSuccess {
                let e = Error::msg("Could not clear the bindings from DuckDB statement");
                log::error!("{e:#}");
                return Err(e);
            }
        }
        Ok(self)
    }
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)?;
        Ok(self)
    }
    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        unsafe {
            let prepared = self.statement();
            let value = v.as_value();
            let state = match value {
                Value::Null
                | Value::Boolean(None, ..)
                | Value::Int8(None, ..)
                | Value::Int16(None, ..)
                | Value::Int32(None, ..)
                | Value::Int64(None, ..)
                | Value::Int128(None, ..)
                | Value::UInt8(None, ..)
                | Value::UInt16(None, ..)
                | Value::UInt32(None, ..)
                | Value::UInt64(None, ..)
                | Value::UInt128(None, ..)
                | Value::Float32(None, ..)
                | Value::Float64(None, ..)
                | Value::Decimal(None, ..)
                | Value::Char(None, ..)
                | Value::Varchar(None, ..)
                | Value::Blob(None, ..)
                | Value::Date(None, ..)
                | Value::Time(None, ..)
                | Value::Timestamp(None, ..)
                | Value::TimestampWithTimezone(None, ..)
                | Value::Interval(None, ..)
                | Value::Uuid(None, ..)
                | Value::Array(None, ..)
                | Value::List(None, ..)
                | Value::Map(None, ..)
                | Value::Struct(None, ..) => duckdb_bind_null(prepared, index),
                Value::Boolean(Some(v), ..) => duckdb_bind_boolean(prepared, index, v),
                Value::Int8(Some(v), ..) => duckdb_bind_int8(prepared, index, v),
                Value::Int16(Some(v), ..) => duckdb_bind_int16(prepared, index, v),
                Value::Int32(Some(v), ..) => duckdb_bind_int32(prepared, index, v),
                Value::Int64(Some(v), ..) => duckdb_bind_int64(prepared, index, v),
                Value::Int128(Some(v), ..) => {
                    duckdb_bind_hugeint(prepared, index, i128_to_duckdb_hugeint(v))
                }
                Value::UInt8(Some(v), ..) => duckdb_bind_uint8(prepared, index, v),
                Value::UInt16(Some(v), ..) => duckdb_bind_uint16(prepared, index, v),
                Value::UInt32(Some(v), ..) => duckdb_bind_uint32(prepared, index, v),
                Value::UInt64(Some(v), ..) => duckdb_bind_uint64(prepared, index, v),
                Value::UInt128(Some(v), ..) => {
                    duckdb_bind_uhugeint(prepared, index, u128_to_duckdb_uhugeint(v))
                }
                Value::Float32(Some(v), ..) => duckdb_bind_float(prepared, index, v),
                Value::Float64(Some(v), ..) => duckdb_bind_double(prepared, index, v),
                Value::Decimal(Some(v), w, s) => {
                    duckdb_bind_decimal(prepared, index, decimal_to_duckdb_decimal(&v, w, s))
                }
                Value::Char(Some(v), ..) => {
                    let v = v.to_string();
                    let status =
                        duckdb_bind_varchar_length(prepared, index, v.as_ptr() as *const i8, 1);
                    status
                }
                Value::Varchar(Some(v), ..) => {
                    let status = duckdb_bind_varchar_length(
                        prepared,
                        index,
                        v.as_ptr() as *const i8,
                        v.len() as u64,
                    );
                    status
                }
                Value::Blob(Some(v), ..) => {
                    let status = duckdb_bind_blob(
                        prepared,
                        index,
                        v.as_ptr() as *const c_void,
                        v.len() as u64,
                    );
                    status
                }
                Value::Date(Some(v), ..) => {
                    duckdb_bind_date(prepared, index, date_to_duckdb_date(&v))
                }
                Value::Time(Some(v), ..) => {
                    duckdb_bind_time(prepared, index, time_to_duckdb_time(&v))
                }
                Value::Timestamp(Some(v), ..) => duckdb_bind_timestamp(
                    prepared,
                    index,
                    primitive_date_time_to_duckdb_timestamp(&v),
                ),
                Value::TimestampWithTimezone(Some(v), ..) => duckdb_bind_timestamp_tz(
                    prepared,
                    index,
                    offsetdatetime_to_duckdb_timestamp(&v),
                ),
                Value::Interval(Some(v), ..) => {
                    duckdb_bind_interval(prepared, index, interval_to_duckdb_interval(&v))
                }
                Value::Uuid(Some(v), ..) => {
                    let v = CBox::new(
                        duckdb_create_uuid(u128_to_duckdb_uhugeint(v.as_u128())),
                        |mut ptr| duckdb_destroy_value(&mut ptr),
                    );
                    duckdb_bind_value(prepared, index, *v)
                }
                _ => {
                    let error =
                        Error::msg(format!("Cannot use a {:?} as a query parameter", value));
                    log::error!("{:#}", error);
                    return Err(error);
                }
            };
            if state != duckdb_state_DuckDBSuccess {
                let error =
                    Error::msg(error_message_from_ptr(&duckdb_prepare_error(prepared)).to_string())
                        .context(format!("While trying to bind the parameter {}", index));
                log::error!("{:#}", error);
                return Err(error);
            }
            self.index = index + 1;
            Ok(self)
        }
    }
}

impl From<CBox<duckdb_prepared_statement>> for DuckDBPrepared {
    fn from(value: CBox<duckdb_prepared_statement>) -> Self {
        Self::new(value)
    }
}
