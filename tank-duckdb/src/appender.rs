use crate::ValueWrap;
use libduckdb_sys::*;
use std::{
    ffi::{c_char, c_void},
    mem, ptr,
};
use tank_core::{Error, Value, error_message_from_ptr};

/// A data chunk column filled by the appender, with its buffers cached for the current chunk.
pub(crate) struct DuckdbAppender {
    vector: duckdb_vector,
    data: *mut c_void,
    validity: *mut u64,
}

impl DuckdbAppender {
    pub(crate) fn new(vector: duckdb_vector) -> Self {
        Self {
            vector,
            data: ptr::null_mut(),
            validity: ptr::null_mut(),
        }
    }

    /// Whether values like `value` can be written directly into a data chunk.
    pub(crate) fn supports(value: &Value) -> bool {
        matches!(
            value,
            Value::Boolean(..)
                | Value::Int8(..)
                | Value::Int16(..)
                | Value::Int32(..)
                | Value::Int64(..)
                | Value::Int128(..)
                | Value::UInt8(..)
                | Value::UInt16(..)
                | Value::UInt32(..)
                | Value::UInt64(..)
                | Value::UInt128(..)
                | Value::Float32(..)
                | Value::Float64(..)
                | Value::Decimal(_, 1.., ..)
                | Value::Char(..)
                | Value::Varchar(..)
                | Value::Blob(..)
                | Value::Date(..)
                | Value::Time(..)
                | Value::Timestamp(..)
                | Value::TimestampWithTimezone(..)
                | Value::Interval(..)
                | Value::Uuid(..)
        )
    }

    /// Writes a value of a [`supported`](Self::supports) type, anything else becomes NULL.
    #[inline(always)]
    pub(crate) fn append(&mut self, row: usize, value: Value<'_>) {
        match value {
            Value::Boolean(Some(v)) => self.write(row, v),
            Value::Int8(Some(v)) => self.write(row, v),
            Value::Int16(Some(v)) => self.write(row, v),
            Value::Int32(Some(v)) => self.write(row, v),
            Value::Int64(Some(v)) => self.write(row, v),
            Value::Int128(Some(v)) => self.write(row, ValueWrap::from_i128(*v)),
            Value::UInt8(Some(v)) => self.write(row, v),
            Value::UInt16(Some(v)) => self.write(row, v),
            Value::UInt32(Some(v)) => self.write(row, v),
            Value::UInt64(Some(v)) => self.write(row, v),
            Value::UInt128(Some(v)) => self.write(row, ValueWrap::from_u128(*v)),
            Value::Float32(Some(v)) => self.write(row, v),
            Value::Float64(Some(v)) => self.write(row, v),
            Value::Decimal(Some(v), width, scale) => {
                let raw = ValueWrap::from_decimal(&v, width, scale).value;
                match width {
                    ..=4 => self.write(row, raw.lower as i16),
                    ..=9 => self.write(row, raw.lower as i32),
                    ..=18 => self.write(row, raw.lower as i64),
                    _ => self.write(row, raw),
                }
            }
            Value::Char(Some(v)) => self.write_bytes(row, v.encode_utf8(&mut [0; 4]).as_bytes()),
            Value::Varchar(Some(v)) => self.write_bytes(row, v.as_bytes()),
            Value::Blob(Some(v)) => self.write_bytes(row, &v),
            Value::Date(Some(v)) => self.write(row, ValueWrap::from_date(&v)),
            Value::Time(Some(v)) => self.write(row, ValueWrap::from_time(&v)),
            Value::Timestamp(Some(v)) => self.write(row, ValueWrap::from_timestamp(&v)),
            Value::TimestampWithTimezone(Some(v)) => {
                self.write(row, ValueWrap::from_timestamptz(&v))
            }
            Value::Interval(Some(v)) => self.write(row, ValueWrap::from_interval(&v)),
            Value::Uuid(Some(v)) => self.write(row, ValueWrap::from_uuid(&v)),
            _ => self.set_null(row),
        }
    }

    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.data = unsafe { duckdb_vector_get_data(self.vector) };
        self.validity = ptr::null_mut();
    }

    #[inline(always)]
    fn write<T>(&self, row: usize, value: T) {
        unsafe { self.data.cast::<T>().add(row).write(value) }
    }

    #[inline(always)]
    fn write_bytes(&self, row: usize, bytes: &[u8]) {
        unsafe {
            if bytes.len() <= 12 {
                let mut raw: duckdb_string_t = mem::zeroed();
                raw.value.inlined.length = bytes.len() as u32;
                ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    raw.value.inlined.inlined.as_mut_ptr() as *mut u8,
                    bytes.len(),
                );
                self.write(row, raw);
            } else {
                duckdb_unsafe_vector_assign_string_element_len(
                    self.vector,
                    row as _,
                    bytes.as_ptr() as *const c_char,
                    bytes.len() as _,
                );
            }
        }
    }

    #[inline(always)]
    fn set_null(&mut self, row: usize) {
        unsafe {
            if self.validity.is_null() {
                duckdb_vector_ensure_validity_writable(self.vector);
                self.validity = duckdb_vector_get_validity(self.vector);
            }
            *self.validity.add(row >> 6) &= !(1 << (row & 63));
        }
    }
}

#[inline(always)]
pub(crate) fn appender_error(appender: duckdb_appender) -> Error {
    Error::msg(error_message_from_ptr(unsafe { duckdb_appender_error(appender) }).into_owned())
}
