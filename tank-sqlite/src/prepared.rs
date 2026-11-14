use crate::{CBox, sql_writer::SQLiteSqlWriter};
use libsqlite3_sys::*;
use rust_decimal::prelude::ToPrimitive;
use std::{
    borrow::Cow,
    ffi::{CStr, c_int},
    fmt::{self, Display},
    hint, slice,
    sync::Arc,
};
use tank_core::{
    AsValue, Context, DynQuery, Error, Fragment, Prepared, Result, SqlValueWriter, Value,
    error_message_from_ptr, truncate_long,
};

/// Prepared statement wrapper for SQLite.
#[derive(Debug)]
pub struct SQLitePrepared {
    pub(crate) statement: CBox<*mut sqlite3_stmt>,
    pub(crate) index: u64,
    /// Column labels of the last result, reused when the statement returns the same shape.
    pub(crate) labels: Option<Arc<[String]>>,
    pub(crate) columns: c_int,
}

impl SQLitePrepared {
    const WRITER: SQLiteSqlWriter = SQLiteSqlWriter {};

    #[inline(always)]
    pub(crate) fn new(statement: CBox<*mut sqlite3_stmt>) -> Self {
        Self {
            statement: statement.into(),
            index: 0,
            labels: None,
            columns: 0,
        }
    }

    #[inline(always)]
    pub(crate) fn statement(&self) -> *mut sqlite3_stmt {
        *self.statement
    }

    /// Number of columns in the current result.
    #[inline(always)]
    pub(crate) fn column_count(&self) -> c_int {
        unsafe { sqlite3_column_count(self.statement()) }
    }

    /// Name of the result column at `index`.
    pub(crate) fn column_name(&self, index: c_int) -> Result<String> {
        unsafe {
            let name = sqlite3_column_name(self.statement(), index);
            if name.is_null() {
                return Err(Error::msg(format!(
                    "Missing SQLite column name at index {index}"
                )));
            }
            Ok(CStr::from_ptr(name).to_string_lossy().into_owned())
        }
    }

    /// Decodes the result column at `index`, borrowing text and blob payloads.
    ///
    /// The borrow is valid until the statement is stepped, reset or finalized.
    #[inline(always)]
    pub(crate) fn extract<'a>(&self, index: c_int) -> Result<Value<'a>> {
        let statement = self.statement();
        unsafe {
            match sqlite3_column_type(statement, index) {
                SQLITE_INTEGER => Ok(Value::Int64(Some(sqlite3_column_int64(statement, index)))),
                SQLITE_FLOAT => Ok(Value::Float64(Some(sqlite3_column_double(
                    statement, index,
                )))),
                SQLITE_TEXT => {
                    let data = sqlite3_column_text(statement, index);
                    if data.is_null() {
                        hint::cold_path();
                        return Err(self.column_error(index));
                    }
                    let len = sqlite3_column_bytes(statement, index) as usize;
                    Ok(Value::Varchar(Some(String::from_utf8_lossy(
                        slice::from_raw_parts(data, len),
                    ))))
                }
                SQLITE_BLOB => {
                    let data = sqlite3_column_blob(statement, index) as *const u8;
                    if data.is_null() {
                        hint::cold_path();
                        // An empty blob may be reported as a null pointer.
                        if sqlite3_column_bytes(statement, index) == 0
                            && sqlite3_errcode(sqlite3_db_handle(statement)) != SQLITE_NOMEM
                        {
                            return Ok(Value::Blob(Some(Cow::Borrowed(&[]))));
                        }
                        return Err(self.column_error(index));
                    }
                    let len = sqlite3_column_bytes(statement, index) as usize;
                    Ok(Value::Blob(Some(Cow::Borrowed(slice::from_raw_parts(
                        data, len,
                    )))))
                }
                SQLITE_NULL => Ok(Value::Null),
                column_type => {
                    hint::cold_path();
                    Err(Error::msg(format!(
                        "Unexpected SQLite column type {column_type} at index {index}"
                    )))
                }
            }
        }
    }

    /// Builds the error for a column SQLite failed to materialize.
    #[cold]
    #[inline(never)]
    fn column_error(&self, index: c_int) -> Error {
        let message =
            unsafe { error_message_from_ptr(sqlite3_errmsg(sqlite3_db_handle(self.statement()))) };
        Error::msg(message.into_owned())
            .context(format!("Could not extract the column at index {index}"))
    }

    /// Binds a temporal value written by the dialect, whose text only lives for this call.
    #[inline]
    unsafe fn bind_written(
        &self,
        index: c_int,
        query: &mut DynQuery,
        write: impl Fn(&mut Context, &mut DynQuery),
    ) -> c_int {
        query.clear();
        write(&mut Context::fragment(Fragment::ParameterBinding), query);
        let v = query.as_str();
        unsafe {
            sqlite3_bind_text(
                self.statement(),
                index,
                v.as_ptr() as _,
                v.len() as _,
                SQLITE_TRANSIENT(),
            )
        }
    }

    #[cold]
    #[inline(never)]
    fn bind_error(&self, index: c_int, rc: c_int) -> Error {
        let sql = unsafe { sqlite3_sql(self.statement()) };
        let sql = if sql.is_null() {
            ""
        } else {
            unsafe { CStr::from_ptr(sql) }
                .to_str()
                .unwrap_or("unprintable")
        };
        let message =
            unsafe { error_message_from_ptr(sqlite3_errmsg(sqlite3_db_handle(self.statement()))) };
        Error::msg(message.into_owned()).context(format!(
            "Cannot bind the parameter {index} (rc {rc}) to query:\n{}",
            truncate_long!(sql)
        ))
    }

    pub fn last_error(&self) -> String {
        unsafe {
            let db = sqlite3_db_handle(self.statement());
            let errcode = sqlite3_errcode(db);
            format!(
                "Error ({errcode}): {}",
                error_message_from_ptr(sqlite3_errmsg(db)),
            )
        }
    }
}

impl Prepared for SQLitePrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn is_empty(&self) -> bool {
        self.statement().is_null()
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.index = 0;
        unsafe {
            let rc = sqlite3_reset(self.statement());
            let error = || {
                let error = Error::msg(self.last_error())
                    .context("Could not clear the bindings from Sqlite statement");
                log::error!("{error:#}");
                error
            };
            if rc != SQLITE_OK {
                return Err(error());
            }
            let rc = sqlite3_clear_bindings(self.statement());
            if rc != SQLITE_OK {
                return Err(error());
            }
        }
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)?;
        Ok(self)
    }

    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        // SQLite's parameter index is 1-based.
        let index = index as c_int + 1;
        let value = v.as_value();
        let statement = self.statement();
        let mut scratch = DynQuery::default();
        #[cold]
        #[inline(never)]
        fn unsupported_value(index: c_int, value: &Value<'_>) -> Error {
            Error::msg(format!(
                "Cannot use {value:?} as the parameter at index {index}"
            ))
        }
        unsafe {
            let rc = if value.is_null() {
                sqlite3_bind_null(statement, index)
            } else {
                match value {
                    Value::Boolean(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                    Value::Int8(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                    Value::Int16(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                    Value::Int32(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                    Value::Int64(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                    Value::Int128(Some(ref v)) => match sqlite3_int64::try_from(**v) {
                        Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                        Err(_) => {
                            hint::cold_path();
                            return Err(unsupported_value(index, &value));
                        }
                    },
                    Value::UInt8(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                    Value::UInt16(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                    Value::UInt32(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                    Value::UInt64(Some(v)) => match sqlite3_int64::try_from(v) {
                        Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                        Err(_) => {
                            hint::cold_path();
                            return Err(unsupported_value(index, &value));
                        }
                    },
                    Value::UInt128(Some(ref v)) => match sqlite3_int64::try_from(**v) {
                        Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                        Err(_) => {
                            hint::cold_path();
                            return Err(unsupported_value(index, &value));
                        }
                    },
                    Value::Float32(Some(v)) => sqlite3_bind_double(statement, index, v as _),
                    Value::Float64(Some(v)) => sqlite3_bind_double(statement, index, v as _),
                    Value::Decimal(Some(v), ..) => match v.to_f64() {
                        Some(v) => sqlite3_bind_double(statement, index, v as _),
                        None => {
                            hint::cold_path();
                            return Err(unsupported_value(index, &value));
                        }
                    },
                    Value::Char(Some(v)) => {
                        let mut buffer = [0u8; 4];
                        let value = v.encode_utf8(&mut buffer);
                        sqlite3_bind_text(
                            statement,
                            index,
                            value.as_ptr() as _,
                            value.len() as _,
                            SQLITE_TRANSIENT(),
                        )
                    }
                    Value::Varchar(Some(v)) => sqlite3_bind_text(
                        statement,
                        index,
                        v.as_ptr() as _,
                        v.len() as _,
                        SQLITE_TRANSIENT(),
                    ),
                    Value::Blob(Some(v)) => sqlite3_bind_blob(
                        statement,
                        index,
                        v.as_ptr() as _,
                        v.len() as _,
                        SQLITE_TRANSIENT(),
                    ),
                    Value::Date(Some(v)) => {
                        self.bind_written(index, &mut scratch, |context, out| {
                            Self::WRITER.write_date(context, out, &v)
                        })
                    }
                    Value::Time(Some(v)) => {
                        self.bind_written(index, &mut scratch, |context, out| {
                            Self::WRITER.write_time(context, out, &v)
                        })
                    }
                    Value::Timestamp(Some(v)) => {
                        self.bind_written(index, &mut scratch, |context, out| {
                            Self::WRITER.write_timestamp(context, out, &v)
                        })
                    }
                    Value::TimestampWithTimezone(Some(v)) => {
                        self.bind_written(index, &mut scratch, |context, out| {
                            Self::WRITER.write_timestamptz(context, out, &v)
                        })
                    }
                    Value::Uuid(Some(v)) => {
                        let mut buffer = [0u8; 36];
                        let v = v.hyphenated().encode_lower(&mut buffer);
                        sqlite3_bind_text(
                            statement,
                            index,
                            v.as_ptr() as _,
                            v.len() as _,
                            SQLITE_TRANSIENT(),
                        )
                    }
                    _ => {
                        hint::cold_path();
                        return Err(unsupported_value(index, &value));
                    }
                }
            };
            if rc != SQLITE_OK {
                hint::cold_path();
                return Err(self.bind_error(index, rc));
            }
        }
        self.index = index as u64;
        Ok(self)
    }
}

impl Display for SQLitePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = unsafe { sqlite3_sql(self.statement()) };
        if ptr.is_null() {
            return write!(f, "{:p}", self.statement());
        }
        let sql = unsafe { CStr::from_ptr(ptr) }.to_string_lossy();
        f.write_str(&sql)
    }
}
