use crate::{CBox, SQLiteRow, row::SQLiteRowData, sql_writer::SQLiteSqlWriter};
use libsqlite3_sys::*;
use rust_decimal::prelude::ToPrimitive;
use std::{
    borrow::Cow,
    ffi::{CStr, c_int},
    fmt::{self, Display},
    hint, mem,
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
    pub(crate) row: Option<Arc<SQLiteRowData>>,
    buffer: DynQuery,
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
            row: None,
            buffer: DynQuery::default(),
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

    #[cold]
    #[inline(never)]
    fn init_labels(&mut self) -> Result<()> {
        let statement = self.statement();
        let columns = self.column_count();
        let recompiled =
            unsafe { sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_REPREPARE, 1) } != 0;
        if self.labels.is_none() || self.columns != columns || recompiled {
            self.labels = Some(
                (0..columns)
                    .map(|index| self.column_name(index))
                    .collect::<Result<Arc<[_]>>>()?,
            );
            self.columns = columns;
            self.row = None;
        }
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn read_row(&mut self, initialize: bool) -> Result<SQLiteRow> {
        if initialize {
            self.init_labels()?;
        }
        let statement = self.statement();
        let labels = self
            .labels
            .as_ref()
            .expect("labels initialized before reading a row");
        SQLiteRow::read(&mut self.row, labels, statement)
    }

    /// Binds a temporal value written by the dialect, whose text only lives for this call.
    #[inline]
    unsafe fn bind_written(
        statement: *mut sqlite3_stmt,
        index: c_int,
        buffer: &mut DynQuery,
        write: impl Fn(&mut Context, &mut DynQuery),
    ) -> c_int {
        buffer.clear();
        write(&mut Context::fragment(Fragment::ParameterBinding), buffer);
        let v = buffer.as_str();
        unsafe {
            sqlite3_bind_text(
                statement,
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

    #[cold]
    #[inline(never)]
    fn unsupported_value(index: c_int, value: Value<'_>) -> Error {
        Error::msg(format!(
            "Cannot use {value:?} as the parameter at index {index}"
        ))
    }

    pub(crate) fn release_bindings(&mut self) -> Result<&mut Self> {
        self.index = 0;
        let rc = unsafe { sqlite3_clear_bindings(self.statement()) };
        if rc != SQLITE_OK {
            return Err(self.bind_error(0, rc));
        }
        Ok(self)
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
        }
        self.release_bindings()
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)?;
        Ok(self)
    }

    #[inline(always)]
    fn bind_index(&mut self, v: impl AsValue, index: u64) -> Result<&mut Self> {
        // SQLite's parameter index is 1-based.
        let index = index as c_int + 1;
        let value = v.as_value();
        let statement = self.statement();
        unsafe {
            let rc = if value.is_null() {
                sqlite3_bind_null(statement, index)
            } else {
                'simple_value: {
                    let rc = match value {
                        Value::Boolean(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int8(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int16(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int32(Some(v)) => sqlite3_bind_int(statement, index, v as _),
                        Value::Int64(Some(v)) => sqlite3_bind_int64(statement, index, v),
                        Value::Int128(Some(ref v)) => {
                            break 'simple_value match sqlite3_int64::try_from(**v) {
                                Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                                Err(_) => {
                                    hint::cold_path();
                                    return Err(Self::unsupported_value(index, value));
                                }
                            };
                        }
                        Value::UInt8(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt16(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt32(Some(v)) => sqlite3_bind_int64(statement, index, v as _),
                        Value::UInt64(Some(v)) => match sqlite3_int64::try_from(v) {
                            Ok(v) => sqlite3_bind_int64(statement, index, v),
                            Err(_) => {
                                hint::cold_path();
                                return Err(Self::unsupported_value(index, value));
                            }
                        },
                        Value::UInt128(Some(ref v)) => {
                            break 'simple_value match sqlite3_int64::try_from(**v) {
                                Ok(v) => sqlite3_bind_int64(statement, index, v as _),
                                Err(_) => {
                                    hint::cold_path();
                                    return Err(Self::unsupported_value(index, value));
                                }
                            };
                        }
                        Value::Float32(Some(v)) => sqlite3_bind_double(statement, index, v as _),
                        Value::Float64(Some(v)) => sqlite3_bind_double(statement, index, v),
                        Value::Decimal(Some(v), ..) => match v.to_f64() {
                            Some(v) => sqlite3_bind_double(statement, index, v as _),
                            None => {
                                hint::cold_path();
                                return Err(Self::unsupported_value(index, value));
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
                        Value::Varchar(Some(ref v)) => {
                            let rc = sqlite3_bind_text(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                SQLITE_TRANSIENT(),
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Blob(Some(ref v)) => {
                            let rc = sqlite3_bind_blob(
                                statement,
                                index,
                                v.as_ptr() as _,
                                v.len() as _,
                                SQLITE_TRANSIENT(),
                            );
                            if matches!(v, Cow::Owned(_)) {
                                break 'simple_value rc;
                            }
                            rc
                        }
                        Value::Date(Some(v)) => Self::bind_written(
                            statement,
                            index,
                            &mut self.buffer,
                            |context, out| Self::WRITER.write_date(context, out, &v),
                        ),
                        Value::Time(Some(v)) => Self::bind_written(
                            statement,
                            index,
                            &mut self.buffer,
                            |context, out| Self::WRITER.write_time(context, out, &v),
                        ),
                        Value::Timestamp(Some(v)) => Self::bind_written(
                            statement,
                            index,
                            &mut self.buffer,
                            |context, out| Self::WRITER.write_timestamp(context, out, &v),
                        ),
                        Value::TimestampWithTimezone(Some(v)) => Self::bind_written(
                            statement,
                            index,
                            &mut self.buffer,
                            |context, out| Self::WRITER.write_timestamptz(context, out, &v),
                        ),
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
                            return Err(Self::unsupported_value(index, value));
                        }
                    };
                    // Avoid an opaque Value::drop call for simple values that don't allocate memory.
                    mem::forget(value);
                    rc
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
