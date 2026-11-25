use crate::{CBox, to_duckdb_decimal, to_duckdb_time, to_duckdb_timestamp};
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, c_char, c_void},
    hint, mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ptr, slice, str,
};
use tank_core::{
    CollectionType, Interval, MapType, StructType, TableRef, Value, ValueOwned, char_ptr_to_string,
    decimal_from_scaled,
};
use time::{Date, PrimitiveDateTime, Time};
use uuid::Uuid;

/// A DuckDB data-chunk vector with cached data and validity buffers for reading or writing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Column {
    data: *mut c_void,
    validity: *mut u64,
    vector: duckdb_vector,
    id: DUCKDB_TYPE,
    decimal_width: u8,
    decimal_scale: u8,
}

impl Column {
    #[inline(always)]
    pub(crate) fn new(vector: duckdb_vector) -> Self {
        unsafe {
            let lt = CBox(duckdb_vector_get_column_type(vector));
            let id = duckdb_get_type_id(*lt);
            let (decimal_width, decimal_scale) = if id == DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
                (duckdb_decimal_width(*lt), duckdb_decimal_scale(*lt))
            } else {
                (0, 0)
            };
            Self {
                data: duckdb_vector_get_data(vector),
                validity: duckdb_vector_get_validity(vector),
                vector,
                id,
                decimal_width,
                decimal_scale,
            }
        }
    }

    pub(crate) const fn supports_append(value: &Value) -> bool {
        value.is_scalar()
            && !matches!(
                value,
                Value::IpAddr(..) | Value::Unknown(..) | Value::Decimal(_, 0, ..)
            )
    }

    #[inline(always)]
    pub(crate) fn rebind(&mut self, vector: duckdb_vector) {
        self.vector = vector;
        self.data = unsafe { duckdb_vector_get_data(vector) };
        self.validity = unsafe { duckdb_vector_get_validity(vector) };
    }

    #[inline(always)]
    pub(crate) fn reset_write(&mut self) {
        self.data = unsafe { duckdb_vector_get_data(self.vector) };
        self.validity = ptr::null_mut();
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { (*self.validity.add(row >> 6) >> (row & 63)) & 1 != 0 }
    }

    #[inline(always)]
    const fn read<T: Copy>(&self, row: usize) -> T {
        unsafe { *self.data.cast::<T>().add(row) }
    }

    #[inline(always)]
    const fn write<T>(&self, row: usize, value: T) {
        unsafe { self.data.cast::<T>().add(row).write(value) }
    }

    #[inline(always)]
    fn write_bytes(&self, row: usize, bytes: &[u8]) {
        unsafe {
            duckdb_unsafe_vector_assign_string_element_len(
                self.vector,
                row as _,
                bytes.as_ptr() as *const c_char,
                bytes.len() as _,
            );
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

    #[inline(always)]
    const fn bytes_slice<'a>(&self, row: usize) -> &'a [u8] {
        unsafe {
            let b = &*self.data.cast::<duckdb_string_t>().add(row);
            let len = b.value.inlined.length as usize;
            let ptr = [
                b.value.pointer.ptr as *const u8,
                b.value.inlined.inlined.as_ptr() as *const u8,
            ][(len <= b.value.inlined.inlined.len()) as usize];
            slice::from_raw_parts(ptr, len)
        }
    }

    /// Typed NULL of a DuckDB type, fetching `logical_type` from `vector` only when needed.
    fn empty_value(
        id: DUCKDB_TYPE,
        lt: duckdb_logical_type,
        vector: duckdb_vector,
    ) -> Option<ValueOwned> {
        let child = |t| {
            let c = CBox(t);
            Self::empty_value(unsafe { duckdb_get_type_id(*c) }, *c, ptr::null_mut())
        };
        Some(match id {
            DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Value::Boolean(None),
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Value::Int8(None),
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Value::Int16(None),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Value::Int32(None),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Value::Int64(None),
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Value::Int128(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Value::UInt8(None),
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Value::UInt16(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Value::UInt32(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Value::UInt64(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => Value::UInt128(None),
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Value::Float32(None),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Value::Float64(None),
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => Value::Date(None),
            DUCKDB_TYPE_DUCKDB_TYPE_TIME
            | DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => Value::Time(None),
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => Value::Timestamp(None),
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => Value::Interval(None),
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR | DUCKDB_TYPE_DUCKDB_TYPE_ENUM => Value::Varchar(None),
            DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Value::Blob(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UUID => Value::Uuid(None),
            _ => unsafe {
                let owned = lt
                    .is_null()
                    .then(|| CBox(duckdb_vector_get_column_type(vector)));
                let lt = owned.as_deref().copied().unwrap_or(lt);
                match id {
                    DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                        Value::Decimal(None, duckdb_decimal_width(lt), duckdb_decimal_scale(lt))
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_LIST => Value::List(
                        None,
                        Box::new(CollectionType {
                            element: child(duckdb_list_type_child_type(lt))?,
                            len: 0,
                        }),
                    ),
                    DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => Value::Array(
                        None,
                        Box::new(CollectionType {
                            element: child(duckdb_array_type_child_type(lt))?,
                            len: duckdb_array_type_array_size(lt) as _,
                        }),
                    ),
                    DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => Value::Struct(
                        None,
                        Box::new(StructType {
                            fields: (0..duckdb_struct_type_child_count(lt))
                                .map(|i| {
                                    Some((
                                        char_ptr_to_string(duckdb_struct_type_child_name(lt, i)),
                                        child(duckdb_struct_type_child_type(lt, i))?,
                                    ))
                                })
                                .collect::<Option<_>>()?,
                            name: TableRef::new(
                                char_ptr_to_string(duckdb_logical_type_get_alias(lt)).into(),
                            ),
                        }),
                    ),
                    DUCKDB_TYPE_DUCKDB_TYPE_MAP => Value::Map(
                        None,
                        Box::new(MapType {
                            key: child(duckdb_map_type_key_type(lt))?,
                            value: child(duckdb_map_type_value_type(lt))?,
                        }),
                    ),
                    _ => return None,
                }
            },
        })
    }

    /// Writes a value of a [`supported`](Self::supports_append) type into `row`, or marks it NULL.
    #[inline(always)]
    pub(crate) fn append(&mut self, row: usize, value: Value<'_>) {
        macro_rules! write_opt {
            ($v:expr) => {
                match $v {
                    Some(v) => self.write(row, v),
                    None => self.set_null(row),
                }
            };
        }
        'simple_value: {
            match value {
                Value::Boolean(v) => write_opt!(v),
                Value::Int32(v) => write_opt!(v),
                Value::Int64(v) => write_opt!(v),
                Value::Float64(v) => write_opt!(v),
                Value::Timestamp(v) => write_opt!(v.as_ref().map(to_duckdb_timestamp)),
                Value::Varchar(ref v) => match v {
                    Some(v) => {
                        self.write_bytes(row, v.as_bytes());
                        if matches!(v, Cow::Owned(_)) {
                            break 'simple_value;
                        }
                    }
                    None => self.set_null(row),
                },
                Value::Null => self.set_null(row),
                other => {
                    append_other(self, row, other);
                    #[inline(never)]
                    fn append_other(col: &mut Column, row: usize, value: Value<'_>) {
                        match value {
                            Value::Int8(Some(v)) => col.write(row, v),
                            Value::Int16(Some(v)) => col.write(row, v),
                            Value::Int128(Some(v)) => col.write(
                                row,
                                duckdb_hugeint {
                                    lower: *v as _,
                                    upper: (*v >> 64) as _,
                                },
                            ),
                            Value::UInt8(Some(v)) => col.write(row, v),
                            Value::UInt16(Some(v)) => col.write(row, v),
                            Value::UInt32(Some(v)) => col.write(row, v),
                            Value::UInt64(Some(v)) => col.write(row, v),
                            Value::UInt128(Some(v)) => col.write(
                                row,
                                duckdb_uhugeint {
                                    lower: *v as _,
                                    upper: (*v >> 64) as _,
                                },
                            ),
                            Value::Float32(Some(v)) => col.write(row, v),
                            Value::Decimal(Some(v), width, scale) => {
                                let raw = to_duckdb_decimal(&v, width, scale).value;
                                match width {
                                    ..=4 => col.write(row, raw.lower as i16),
                                    ..=9 => col.write(row, raw.lower as i32),
                                    ..=18 => col.write(row, raw.lower as i64),
                                    _ => col.write(row, raw),
                                }
                            }
                            Value::Char(Some(v)) => {
                                col.write_bytes(row, v.encode_utf8(&mut [0; 4]).as_bytes())
                            }
                            Value::Blob(Some(v)) => col.write_bytes(row, &v),
                            Value::Date(Some(v)) => {
                                col.write(row, v.to_julian_day() - Interval::EPOCH_JULIAN_DAY)
                            }
                            Value::Time(Some(v)) => col.write(row, to_duckdb_time(&v)),
                            Value::TimestampWithTimezone(Some(v)) => {
                                col.write(row, (v.unix_timestamp_nanos() / 1_000) as i64)
                            }
                            Value::Interval(Some(v)) => col.write(
                                row,
                                duckdb_interval {
                                    months: v.months,
                                    days: v.days,
                                    micros: v.nanos / 1_000,
                                },
                            ),
                            Value::Uuid(Some(v)) => {
                                // DuckDB stores UUIDs as HUGEINT with the sign bit flipped.
                                let (upper, lower) = v.as_u64_pair();
                                col.write(
                                    row,
                                    duckdb_hugeint {
                                        lower,
                                        upper: (upper ^ (1 << 63)) as _,
                                    },
                                );
                            }
                            _ => col.set_null(row),
                        }
                    }
                    break 'simple_value;
                }
            }
            // Avoid an opaque Value::drop call for simple values that do not allocate memory.
            mem::forget(value);
        }
    }

    /// Decodes the 5 hottest scalar types inline, keeping everything else out-of-line for I-cache fit.
    #[inline(always)]
    pub(crate) fn extract<'a>(&self, row: usize) -> Option<Cow<'a, Value<'a>>> {
        let valid = self.is_valid(row);
        let value = match self.id {
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Value::Boolean(valid.then(|| self.read(row))),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Value::Int32(valid.then(|| self.read(row))),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Value::Int64(valid.then(|| self.read(row))),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Value::Float64(valid.then(|| self.read(row))),
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Value::Varchar(valid.then(|| {
                Cow::Borrowed(unsafe { str::from_utf8_unchecked(self.bytes_slice(row)) })
            })),
            _ => {
                // Return `Option<Value<'_>>` (different from `extract`) so hot arms stay in registers.
                let res = complex(self, row);
                #[inline(never)]
                fn complex<'a>(col: &Column, row: usize) -> Option<Value<'a>> {
                    const DAY_MICROS: u64 = Interval::MICROS_IN_DAY as u64;
                    const DAY_NANOS: i64 = Interval::NANOS_IN_DAY as i64;
                    let to_date = |days: i32| {
                        Date::from_julian_day(days.saturating_add(Interval::EPOCH_JULIAN_DAY)).ok()
                    };
                    let to_time = |nanos: u64| {
                        if nanos >= DAY_NANOS as u64 {
                            return None;
                        }
                        let secs = (nanos / 1_000_000_000) as u32;
                        let ns = (nanos - secs as u64 * 1_000_000_000) as u32;
                        let mins = secs / 60;
                        let hour = (mins / 60) as u8;
                        Time::from_hms_nano(
                            hour,
                            (mins - hour as u32 * 60) as u8,
                            (secs - mins * 60) as u8,
                            ns,
                        )
                        .ok()
                    };
                    let to_i128 = |v: duckdb_hugeint| (v.upper as i128) << 64 | v.lower as i128;

                    if !col.is_valid(row)
                        || col.data.is_null()
                            && !matches!(
                                col.id,
                                DUCKDB_TYPE_DUCKDB_TYPE_ARRAY | DUCKDB_TYPE_DUCKDB_TYPE_STRUCT
                            )
                    {
                        return Column::empty_value(col.id, ptr::null_mut(), col.vector);
                    }
                    Some(match col.id {
                        DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
                        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Value::Int8(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Value::Int16(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                            Value::Int128(Some(to_i128(col.read(row)).into()))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Value::UInt8(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Value::UInt16(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Value::UInt32(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Value::UInt64(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                            let v: duckdb_uhugeint = col.read(row);
                            Value::UInt128(Some(((v.upper as u128) << 64 | v.lower as u128).into()))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Value::Float32(Some(col.read(row))),
                        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Value::Decimal(
                            decimal_from_scaled(
                                match col.decimal_width {
                                    ..=4 => col.read::<i16>(row) as _,
                                    ..=9 => col.read::<i32>(row) as _,
                                    ..=18 => col.read::<i64>(row) as _,
                                    _ => to_i128(col.read(row)),
                                },
                                col.decimal_scale as u32,
                            )
                            .ok(),
                            col.decimal_width,
                            col.decimal_scale,
                        ),
                        DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
                            Value::Date(to_date(col.read::<duckdb_date>(row).days))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_TIME
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                            Value::Time(Some(to_time(match col.id {
                                DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => col.read::<i64>(row) as u64,
                                DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                                    (col.read::<u64>(row) >> 24).saturating_mul(1_000)
                                }
                                _ => (col.read::<i64>(row) as u64).saturating_mul(1_000),
                            })?))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS
                        | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                            let raw: i64 = col.read(row);
                            let (div, scale) = match col.id {
                                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => (DAY_NANOS, 1),
                                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                                    (Interval::SECS_IN_DAY, 1_000_000_000)
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
                                    (Interval::SECS_IN_DAY * 1_000, 1_000_000)
                                }
                                _ => (DAY_MICROS as i64, 1_000),
                            };
                            Value::Timestamp(Some(PrimitiveDateTime::new(
                                to_date(i32::try_from(raw.div_euclid(div)).ok()?)?,
                                to_time(raw.rem_euclid(div) as u64 * scale)?,
                            )))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                            let v: duckdb_interval = col.read(row);
                            Value::Interval(Some(Interval::new(v.months, v.days, v.micros * 1_000)))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_ENUM => unsafe {
                            let lt = CBox(duckdb_vector_get_column_type(col.vector));
                            let id = match duckdb_enum_internal_type(*lt) {
                                DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => col.read::<u8>(row) as u64,
                                DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => col.read::<u16>(row) as u64,
                                _ => col.read::<u32>(row) as u64,
                            };
                            let val = CBox(duckdb_enum_dictionary_value(*lt, id));
                            if val.is_null() {
                                hint::cold_path();
                                return None;
                            }
                            Value::Varchar(Some(Cow::Owned(
                                CStr::from_ptr(*val).to_str().ok()?.to_owned(),
                            )))
                        },
                        DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
                            Value::Blob(Some(Cow::Borrowed(col.bytes_slice(row))))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
                            let v: duckdb_hugeint = col.read(row);
                            Value::Uuid(Some(Uuid::from_u64_pair(
                                v.upper as u64 ^ (1 << 63),
                                v.lower,
                            )))
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                            let (Value::List(_, t) | Value::Array(_, t)) =
                                Column::empty_value(col.id, ptr::null_mut(), col.vector)?
                            else {
                                return None;
                            };
                            let (child, begin, len, make): (_, _, _, fn(_, _) -> _) =
                                if col.id == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                                    let e: duckdb_list_entry = col.read(row);
                                    (
                                        unsafe { duckdb_list_vector_get_child(col.vector) },
                                        e.offset as usize,
                                        e.length as usize,
                                        Value::List,
                                    )
                                } else {
                                    (
                                        unsafe { duckdb_array_vector_get_child(col.vector) },
                                        row * t.len as usize,
                                        t.len as usize,
                                        Value::Array,
                                    )
                                };
                            let child = Column::new(child);
                            make(
                                Some(
                                    (begin..begin + len)
                                        .map(|i| child.extract(i).map(Cow::into_owned))
                                        .collect::<Option<_>>()?,
                                ),
                                t,
                            )
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                            let Value::Struct(_, struct_type) =
                                Column::empty_value(col.id, ptr::null_mut(), col.vector)?
                            else {
                                return None;
                            };
                            let child = |i| {
                                Column::new(unsafe {
                                    duckdb_struct_vector_get_child(col.vector, i as _)
                                })
                                .extract(row)
                                .map(Cow::into_owned)
                            };
                            if struct_type.name.name == "INET" {
                                let (
                                    Value::UInt8(Some(ip_type)),
                                    Value::Int128(Some(address)),
                                    Value::UInt16(Some(mask)),
                                ) = (child(0)?, child(1)?, child(2)?)
                                else {
                                    return None;
                                };
                                let (ip, default_mask) = match ip_type {
                                    1 => (IpAddr::V4(Ipv4Addr::from(*address as u32)), 32),
                                    2 => (
                                        IpAddr::V6(Ipv6Addr::from(
                                            (*address as u128) ^ (1_u128 << 127),
                                        )),
                                        128,
                                    ),
                                    _ => return None,
                                };
                                if mask == default_mask {
                                    Value::IpAddr(Some(ip))
                                } else {
                                    Value::Varchar(Some(Cow::Owned(format!("{ip}/{mask}"))))
                                }
                            } else {
                                Value::Struct(
                                    Some(
                                        struct_type
                                            .fields
                                            .iter()
                                            .enumerate()
                                            .map(|(i, (name, _))| Some((name.clone(), child(i)?)))
                                            .collect::<Option<_>>()?,
                                    ),
                                    struct_type,
                                )
                            }
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                            let Value::Map(_, map_type) =
                                Column::empty_value(col.id, ptr::null_mut(), col.vector)?
                            else {
                                return None;
                            };
                            let e: duckdb_list_entry = col.read(row);
                            let entries = unsafe { duckdb_list_vector_get_child(col.vector) };
                            let keys =
                                Column::new(unsafe { duckdb_struct_vector_get_child(entries, 0) });
                            let vals =
                                Column::new(unsafe { duckdb_struct_vector_get_child(entries, 1) });
                            Value::Map(
                                Some(Box::new(
                                    (e.offset as usize..(e.offset + e.length) as usize)
                                        .map(|i| {
                                            Some((
                                                keys.extract(i)?.into_owned(),
                                                vals.extract(i)?.into_owned(),
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
                return res.map(Cow::Owned);
            }
        };
        Some(Cow::Owned(value))
    }
}
