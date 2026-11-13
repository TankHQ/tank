use crate::{ValueWrap, util::CBox};
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, c_void},
    hint, str,
};
use tank_core::{Interval, Value, ValueOwned};
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

    fn empty_value(&self) -> Option<ValueOwned> {
        ValueWrap::empty_value(self.id, || {
            CBox(unsafe { duckdb_vector_get_column_type(self.vector) })
        })
    }

    /// Decodes the common scalar types inline, NULLs included, as typed values.
    #[inline(always)]
    pub(crate) fn extract<'a>(&self, row: usize) -> Option<Cow<'a, Value<'a>>> {
        let data = self.data;
        let valid = self.is_valid(row);
        unsafe {
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
                DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                    Value::Float32(valid.then(|| *(data as *const f32).add(row)))
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
                                    ..=4 => *(data as *const i16).add(row) as _,
                                    ..=9 => *(data as *const i32).add(row) as _,
                                    ..=18 => *(data as *const i64).add(row) as _,
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
                _ => {
                    #[inline(never)]
                    fn complex<'a>(column: &DuckDBColumn, row: usize) -> Option<Value<'a>> {
                        unsafe {
                            let data = column.data;
                            if !column.is_valid(row)
                                || data.is_null()
                                    && !matches!(
                                        column.id,
                                        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY
                                            | DUCKDB_TYPE_DUCKDB_TYPE_STRUCT
                                    )
                            {
                                return column.empty_value();
                            }
                            Some(match column.id {
                                DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
                                DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => {
                                    Value::Int8(Some(*(data as *const i8).add(row)))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                                    Value::Int16(Some(*(data as *const i16).add(row)))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Value::Int128(Some(
                                    ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row))
                                        .into(),
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
                                    Value::UInt128(Some(
                                        ((v.upper as u128) << 64 | v.lower as u128).into(),
                                    ))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_TIME
                                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS
                                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                                    Value::Time(Some(ValueWrap::to_time_ns(match column.id {
                                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => {
                                            *(data as *const i64).add(row)
                                        }
                                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                                            ((*(data as *const u64).add(row) >> 24) as i64)
                                                .saturating_mul(1_000)
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
                                    let days =
                                        i32::try_from(raw.div_euclid(86_400_000_000_000)).ok()?;
                                    Value::Timestamp(Some(PrimitiveDateTime::new(
                                        ValueWrap::to_date(days)?,
                                        ValueWrap::to_time_ns(raw.rem_euclid(86_400_000_000_000))?,
                                    )))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                                    let v = *(data as *const duckdb_interval).add(row);
                                    Value::Interval(Some(Interval::new(
                                        v.months,
                                        v.days,
                                        v.micros * 1_000,
                                    )))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {
                                    let logical_type =
                                        CBox(duckdb_vector_get_column_type(column.vector));
                                    let id = match duckdb_enum_internal_type(*logical_type) {
                                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                                            *(data as *const u8).add(row) as u64
                                        }
                                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                                            *(data as *const u16).add(row) as u64
                                        }
                                        _ => *(data as *const u32).add(row) as u64,
                                    };
                                    let value =
                                        CBox(duckdb_enum_dictionary_value(*logical_type, id));
                                    if value.is_null() {
                                        hint::cold_path();
                                        return None;
                                    }
                                    Value::Varchar(Some(Cow::Owned(
                                        CStr::from_ptr(*value).to_str().ok()?.to_owned(),
                                    )))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
                                    Value::Blob(Some(Cow::Borrowed(ValueWrap::bytes_slice(
                                        &*(data as *const duckdb_string_t).add(row),
                                    ))))
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_UUID => Value::Uuid(Some(
                                    ValueWrap::to_uuid(&*(data as *const duckdb_hugeint).add(row)),
                                )),
                                DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                                    let (Value::List(_, t) | Value::Array(_, t)) =
                                        column.empty_value()?
                                    else {
                                        return None;
                                    };
                                    let (child, begin, len, make_value): (_, _, _, fn(_, _) -> _) =
                                        if column.id == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                                            let e = *(data as *const duckdb_list_entry).add(row);
                                            (
                                                duckdb_list_vector_get_child(column.vector),
                                                e.offset as usize,
                                                e.length as usize,
                                                Value::List,
                                            )
                                        } else {
                                            (
                                                duckdb_array_vector_get_child(column.vector),
                                                row * t.len as usize,
                                                t.len as usize,
                                                Value::Array,
                                            )
                                        };
                                    let col = DuckDBColumn::new(child);
                                    make_value(
                                        Some(
                                            (begin..begin + len)
                                                .map(|i| col.extract(i).map(Cow::into_owned))
                                                .collect::<Option<Box<[_]>>>()?,
                                        ),
                                        t,
                                    )
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                                    let Value::Struct(_, struct_type) = column.empty_value()?
                                    else {
                                        return None;
                                    };
                                    Value::Struct(
                                        Some(
                                            struct_type
                                                .fields
                                                .iter()
                                                .enumerate()
                                                .map(|(i, (name, _))| {
                                                    let child = duckdb_struct_vector_get_child(
                                                        column.vector,
                                                        i as _,
                                                    );
                                                    Some((
                                                        name.clone(),
                                                        DuckDBColumn::new(child)
                                                            .extract(row)?
                                                            .into_owned(),
                                                    ))
                                                })
                                                .collect::<Option<Box<[_]>>>()?,
                                        ),
                                        struct_type,
                                    )
                                }
                                DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                                    let Value::Map(_, map_type) = column.empty_value()? else {
                                        return None;
                                    };
                                    let e = *(data as *const duckdb_list_entry).add(row);
                                    let entries = duckdb_list_vector_get_child(column.vector);
                                    let keys = DuckDBColumn::new(duckdb_struct_vector_get_child(
                                        entries, 0,
                                    ));
                                    let values = DuckDBColumn::new(duckdb_struct_vector_get_child(
                                        entries, 1,
                                    ));
                                    Value::Map(
                                        Some(Box::new(
                                            (e.offset as usize..(e.offset + e.length) as usize)
                                                .map(|i| {
                                                    Some((
                                                        keys.extract(i)?.into_owned(),
                                                        values.extract(i)?.into_owned(),
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
                    complex(self, row).unwrap_or(Value::Null)
                }
            }))
        }
    }
}
