use anyhow::anyhow;
use libsqlite3_sys::*;
use std::ffi::{CStr, c_int};
use tank_core::{AsValue, Result, Value, ValueOwned};

pub(crate) fn extract_value(statement: *mut sqlite3_stmt, index: c_int) -> Result<ValueOwned> {
    unsafe {
        let column_type = sqlite3_column_type(statement, index);
        Ok(match column_type {
            SQLITE_NULL => Value::Null,
            SQLITE_INTEGER => sqlite3_column_int64(statement, index).as_value(),
            SQLITE_FLOAT => sqlite3_column_double(statement, index).as_value(),
            SQLITE_BLOB => {
                let ptr = sqlite3_column_blob(statement, index) as *const u8;
                let len = sqlite3_column_bytes(statement, index) as usize;
                if ptr.is_null()
                    && (len > 0 || sqlite3_errcode(sqlite3_db_handle(statement)) == SQLITE_NOMEM)
                {
                    return Err(anyhow!("Could not extract SQLite blob at index {index}"));
                }
                Value::Blob(Some((0..len).map(|i| *ptr.add(i)).collect()))
            }
            SQLITE_TEXT => {
                let ptr = sqlite3_column_text(statement, index);
                let len = sqlite3_column_bytes(statement, index) as usize;
                if ptr.is_null() {
                    return Err(anyhow!("Could not extract SQLite text at index {index}"));
                }
                String::from_utf8_lossy(&(0..len).map(|i| *ptr.add(i)).collect::<Vec<u8>>())
                    .into_owned()
                    .as_value()
            }
            _ => {
                return Err(anyhow!("Unexpected column type {column_type}"));
            }
        })
    }
}

pub(crate) fn extract_name(statement: *mut sqlite3_stmt, index: c_int) -> Result<String> {
    unsafe {
        let name = sqlite3_column_name(statement, index);
        if name.is_null() {
            return Err(anyhow!("Missing SQLite column name at index {index}"));
        }
        Ok(CStr::from_ptr(name).to_str()?.into())
    }
}
