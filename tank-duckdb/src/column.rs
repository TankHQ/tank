use crate::{ValueWrap, util::CBox};
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, c_char, c_void},
    mem, ptr, str,
};
use tank_core::{Interval, Value};
use time::PrimitiveDateTime;

/// A result vector decoded into values, with its buffers cached for the current chunk.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DuckDBColumn {
    vector: duckdb_vector,
    data: *const c_void,
    validity: *mut u64,
    id: DUCKDB_TYPE,
    decimal_width: u8,
    decimal_scale: u8,
}

impl DuckDBColumn {
    #[inline(always)]
    pub(crate) fn new(vector: duckdb_vector) -> Self {
        unsafe {
            let logical_type = CBox(duckdb_vector_get_column_type(vector));
            let id = duckdb_get_type_id(*logical_type);
            let (width, scale) = if id == DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
                (
                    duckdb_decimal_width(*logical_type),
                    duckdb_decimal_scale(*logical_type),
                )
            } else {
                (0, 0)
            };
            Self {
                vector,
                data: duckdb_vector_get_data(vector) as *const c_void,
                validity: duckdb_vector_get_validity(vector),
                id,
                decimal_width: width,
                decimal_scale: scale,
            }
        }
    }

    /// The same column in the vector of another chunk of the result.
    #[inline(always)]
    pub(crate) fn rebind(&self, vector: duckdb_vector) -> Self {
        unsafe {
            Self {
                vector,
                data: duckdb_vector_get_data(vector) as *const c_void,
                validity: duckdb_vector_get_validity(vector),
                ..*self
            }
        }
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { (*self.validity.add(row >> 6) >> (row & 63)) & 1 != 0 }
    }

    unsafe fn empty_value(&self) -> Option<Value<'static>> {
        unsafe {
            ValueWrap::empty_value(self.id, || CBox(duckdb_vector_get_column_type(self.vector)))
        }
    }

    /// Decodes the common scalar types inline, NULLs included, as typed values.
    #[inline(always)]
    pub(crate) unsafe fn value<'a>(&self, row: usize) -> Option<Cow<'a, Value<'a>>> {
        let data = self.data;
        let valid = self.is_valid(row);
        unsafe {
            // Flat vectors of these types always carry a data buffer. Values that do not fit
            // the Rust type (out of range dates, decimals) decode as NULL.
            Some(Cow::Owned(match self.id {
                DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
                    Value::Boolean(valid.then(|| *(data as *const bool).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                    Value::Int32(valid.then(|| *(data as *const i32).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                    Value::Int64(valid.then(|| *(data as *const i64).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
                    Value::Float64(valid.then(|| *(data as *const f64).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Value::Varchar(valid.then(|| {
                    Cow::Borrowed(str::from_utf8_unchecked(ValueWrap::bytes_slice(
                        &*(data as *const duckdb_string_t).add(row),
                    )))
                })),
                DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Value::Decimal(
                    valid
                        .then(|| {
                            ValueWrap::to_decimal(
                                match self.decimal_width {
                                    ..=4 => *(data as *const i16).add(row) as i128,
                                    ..=9 => *(data as *const i32).add(row) as i128,
                                    ..=18 => *(data as *const i64).add(row) as i128,
                                    _ => ValueWrap::to_i128(
                                        &*(data as *const duckdb_hugeint).add(row),
                                    ),
                                },
                                self.decimal_scale,
                            )
                        })
                        .flatten(),
                    self.decimal_width,
                    self.decimal_scale,
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_DATE => Value::Date(
                    valid
                        .then(|| ValueWrap::to_date((*(data as *const duckdb_date).add(row)).days))
                        .flatten(),
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => Value::Timestamp(
                    valid
                        .then(|| {
                            ValueWrap::to_timestamp(
                                (*(data as *const duckdb_timestamp).add(row)).micros,
                            )
                        })
                        .flatten(),
                ),
                _ => self.decode(row).unwrap_or(Value::Null),
            }))
        }
    }

    #[inline(never)]
    unsafe fn decode<'a>(&self, row: usize) -> Option<Value<'a>> {
        unsafe {
            let data = self.data;
            let id = self.id;
            if !self.is_valid(row)
                || (data.is_null()
                    && !matches!(
                        id,
                        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY | DUCKDB_TYPE_DUCKDB_TYPE_STRUCT
                    ))
            {
                return self.empty_value();
            }
            Some(match id {
                DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
                DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Value::Int8(Some(*(data as *const i8).add(row))),
                DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                    Value::Int16(Some(*(data as *const i16).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Value::Int128(Some(
                    ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)).into(),
                )),
                DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                    Value::UInt8(Some(*(data as *const u8).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                    Value::UInt16(Some(*(data as *const u16).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                    Value::UInt32(Some(*(data as *const u32).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                    Value::UInt64(Some(*(data as *const u64).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                    let v = &*(data as *const duckdb_uhugeint).add(row);
                    Value::UInt128(Some(((v.upper as u128) << 64 | v.lower as u128).into()))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                    Value::Float32(Some(*(data as *const f32).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIME
                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS
                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                    Value::Time(Some(ValueWrap::to_time_ns(match id {
                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => *(data as *const i64).add(row),
                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                            ((*(data as *const u64).add(row) >> 24) as i64).saturating_mul(1_000)
                        }
                        _ => (*(data as *const i64).add(row)).saturating_mul(1_000),
                    })?))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => Value::Timestamp(Some(
                    ValueWrap::to_timestamp(*(data as *const i64).add(row))?,
                )),
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                    Value::Timestamp(Some(ValueWrap::to_timestamp(
                        (*(data as *const i64).add(row)).saturating_mul(1_000_000),
                    )?))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
                    Value::Timestamp(Some(ValueWrap::to_timestamp(
                        (*(data as *const i64).add(row)).saturating_mul(1_000),
                    )?))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                    let raw = *(data as *const i64).add(row);
                    let days = i32::try_from(raw.div_euclid(86_400_000_000_000)).ok()?;
                    Value::Timestamp(Some(PrimitiveDateTime::new(
                        ValueWrap::to_date(days)?,
                        ValueWrap::to_time_ns(raw.rem_euclid(86_400_000_000_000))?,
                    )))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                    let v = *(data as *const duckdb_interval).add(row);
                    Value::Interval(Some(Interval::new(v.months, v.days, v.micros * 1_000)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {
                    let logical_type = CBox(duckdb_vector_get_column_type(self.vector));
                    let idx = match duckdb_enum_internal_type(*logical_type) {
                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => *(data as *const u8).add(row) as u64,
                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => *(data as *const u16).add(row) as u64,
                        _ => *(data as *const u32).add(row) as u64,
                    };
                    let val = CBox(duckdb_enum_dictionary_value(*logical_type, idx));
                    if val.is_null() {
                        return None;
                    }
                    Value::Varchar(Some(Cow::Owned(
                        CStr::from_ptr(*val).to_str().ok()?.to_owned(),
                    )))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Value::Blob(Some(Cow::Borrowed(
                    ValueWrap::bytes_slice(&*(data as *const duckdb_string_t).add(row)),
                ))),
                DUCKDB_TYPE_DUCKDB_TYPE_UUID => Value::Uuid(Some(ValueWrap::to_uuid(
                    &*(data as *const duckdb_hugeint).add(row),
                ))),
                DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                    let (Value::List(_, t) | Value::Array(_, t)) = self.empty_value()? else {
                        return None;
                    };
                    let (child, begin, len, ctor): (_, _, _, fn(_, _) -> _) =
                        if id == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                            let e = *(data as *const duckdb_list_entry).add(row);
                            (
                                duckdb_list_vector_get_child(self.vector),
                                e.offset as usize,
                                e.length as usize,
                                Value::List,
                            )
                        } else {
                            (
                                duckdb_array_vector_get_child(self.vector),
                                row * t.len as usize,
                                t.len as usize,
                                Value::Array,
                            )
                        };
                    let col = Self::new(child);
                    ctor(
                        Some(
                            (begin..begin + len)
                                .map(|i| col.value(i).map(Cow::into_owned))
                                .collect::<Option<Box<[_]>>>()?,
                        ),
                        t,
                    )
                }
                DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                    let Value::Struct(_, struct_type) = self.empty_value()? else {
                        return None;
                    };
                    Value::Struct(
                        Some(
                            struct_type
                                .fields
                                .iter()
                                .enumerate()
                                .map(|(i, (name, _))| {
                                    let child = duckdb_struct_vector_get_child(self.vector, i as _);
                                    Some((name.clone(), Self::new(child).value(row)?.into_owned()))
                                })
                                .collect::<Option<Box<[_]>>>()?,
                        ),
                        struct_type,
                    )
                }
                DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                    let Value::Map(_, map_type) = self.empty_value()? else {
                        return None;
                    };
                    let e = *(data as *const duckdb_list_entry).add(row);
                    let entries = duckdb_list_vector_get_child(self.vector);
                    let keys = Self::new(duckdb_struct_vector_get_child(entries, 0));
                    let values = Self::new(duckdb_struct_vector_get_child(entries, 1));
                    Value::Map(
                        Some(Box::new(
                            (e.offset as usize..(e.offset + e.length) as usize)
                                .map(|i| {
                                    Some((
                                        keys.value(i)?.into_owned(),
                                        values.value(i)?.into_owned(),
                                    ))
                                })
                                .collect::<Option<HashMap<_, _>>>()?,
                        )),
                        map_type,
                    )
                }
                _ => return None,
            })
        }
    }
}

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
                | Value::Decimal(_, 1.., _)
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
    pub(crate) unsafe fn append(&mut self, row: usize, value: Value<'_>) {
        unsafe {
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
                Value::Char(Some(v)) => {
                    self.write_bytes(row, v.encode_utf8(&mut [0; 4]).as_bytes())
                }
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
    }

    #[inline(always)]
    pub(crate) unsafe fn reset(&mut self) {
        self.data = unsafe { duckdb_vector_get_data(self.vector) };
        self.validity = ptr::null_mut();
    }

    #[inline(always)]
    unsafe fn write<T>(&self, row: usize, value: T) {
        unsafe { self.data.cast::<T>().add(row).write(value) }
    }

    #[inline(always)]
    unsafe fn write_bytes(&self, row: usize, bytes: &[u8]) {
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
    unsafe fn set_null(&mut self, row: usize) {
        unsafe {
            if self.validity.is_null() {
                duckdb_vector_ensure_validity_writable(self.vector);
                self.validity = duckdb_vector_get_validity(self.vector);
            }
            *self.validity.add(row >> 6) &= !(1 << (row & 63));
        }
    }
}
