use crate::{cbox::CBox, duckdb_hugeint_to_i128, duckdb_uhugeint_to_u128};
use libduckdb_sys::*;
use rust_decimal::Decimal;
use std::{
    ffi::{CStr, c_void},
    ptr, slice,
};
use tank_core::{Error, Interval, Result, TableRef, Value};
use uuid::Uuid;

pub(crate) fn convert_date(date: duckdb_date_struct) -> Result<time::Date> {
    time::Date::from_calendar_date(
        date.year,
        (date.month as u8).try_into().unwrap(),
        date.day as u8,
    )
    .map_err(|e| Error::new(e).context("Error while creating extracting a date value"))
}

pub(crate) fn convert_time(time: duckdb_time_struct) -> Result<time::Time> {
    time::Time::from_hms_micro(
        time.hour as u8,
        time.min as u8,
        time.sec as u8,
        time.micros as u32,
    )
    .map_err(|e| Error::new(e).context("Error while creating extracting a time value"))
}

pub(crate) fn extract_value(
    vector: duckdb_vector,
    row: usize,
    logical_type: duckdb_logical_type,
    type_id: u32,
    data: *const c_void,
    validity: *mut u64,
) -> Result<Value> {
    unsafe {
        let has_data = !data.is_null() || type_id == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY;
        let is_valid =
            !vector.is_null() && has_data && duckdb_validity_row_is_valid(validity, row as u64);
        let result = match type_id {
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Value::Boolean(if is_valid {
                Some(*(data as *const bool).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Value::Int8(if is_valid {
                Some(*(data as *const i8).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Value::Int16(if is_valid {
                Some(*(data as *const i16).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Value::Int32(if is_valid {
                Some(*(data as *const i32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Value::Int64(if is_valid {
                Some(*(data as *const i64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Value::UInt8(if is_valid {
                Some(*(data as *const u8).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Value::UInt16(if is_valid {
                Some(*(data as *const u16).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Value::UInt32(if is_valid {
                Some(*(data as *const u32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Value::UInt64(if is_valid {
                Some(*(data as *const u64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Value::Int128(if is_valid {
                let data = *(data as *const duckdb_hugeint).add(row);
                duckdb_hugeint_to_i128(&data).into()
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => Value::UInt128(if is_valid {
                let data = *(data as *const duckdb_uhugeint).add(row);
                duckdb_uhugeint_to_u128(&data).into()
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Value::Float32(if is_valid {
                Some(*(data as *const f32).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Value::Float64(if is_valid {
                Some(*(data as *const f64).add(row))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => Value::Timestamp(if is_valid {
                let data = *(data as *const duckdb_timestamp).add(row);
                let date_time =
                    time::OffsetDateTime::from_unix_timestamp_nanos(data.micros as i128 * 1000)
                        .map_err(|e| Error::new(e).context("Error while creating a timestamp"))?;
                Some(time::PrimitiveDateTime::new(
                    date_time.date(),
                    date_time.time(),
                ))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => Value::Date(if is_valid {
                Some(convert_date(duckdb_from_date(
                    *(data as *const duckdb_date).add(row),
                ))?)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => Value::Time(if is_valid {
                Some(convert_time(duckdb_from_time(
                    *(data as *const duckdb_time).add(row),
                ))?)
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => Value::Interval(if is_valid {
                Some(convert_interval(*(data as *const duckdb_interval).add(row)))
            } else {
                None
            }),
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR | DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
                let value = if is_valid {
                    let data = *(data as *const duckdb_string_t).add(row);
                    let parts = if duckdb_string_is_inlined(data) {
                        (
                            &data.value.inlined.inlined as *const i8,
                            data.value.inlined.length,
                        )
                    } else {
                        (
                            data.value.pointer.ptr as *const i8,
                            data.value.pointer.length,
                        )
                    };
                    Some(slice::from_raw_parts(
                        parts.0 as *const u8,
                        parts.1 as usize,
                    ))
                } else {
                    None
                };
                if type_id == DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR {
                    Value::Varchar(value.map(|v| String::from_utf8_unchecked(v.into())))
                } else {
                    Value::Blob(value.map(|v| v.into()))
                }
            }
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                let width = duckdb_decimal_width(logical_type);
                let scale = duckdb_decimal_scale(logical_type);
                Value::Decimal(
                    if is_valid {
                        let num = match duckdb_decimal_internal_type(logical_type) {
                            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                                *(data as *const i16).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                                *(data as *const i32).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                                *(data as *const i64).add(row) as i128
                            }
                            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                                *(data as *const i128).add(row) as i128
                            }
                            _ => {
                                let error = Error::msg("Invalid internal decimal storage type");
                                log::error!("{:#}", error);
                                return Err(error);
                            }
                        };
                        Some(Decimal::from_i128_with_scale(num, scale as u32))
                    } else {
                        None
                    },
                    width,
                    scale,
                )
            }
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => Value::Timestamp(if is_valid {
                let data = duckdb_from_timestamp(*(data as *const duckdb_timestamp).add(row));
                data.time;
                Some(time::PrimitiveDateTime::new(
                    convert_date(data.date)?,
                    convert_time(data.time)?,
                ))
            } else {
                None
            }),
            //  DUCKDB_TYPE_DUCKDB_TYPE_ENUM =>
            DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                let is_array = type_id == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY;
                let (vector, child_logical_type) = if is_array {
                    (
                        duckdb_array_vector_get_child(vector), // vector will be the child vector
                        duckdb_array_type_child_type(logical_type),
                    )
                } else {
                    (
                        duckdb_list_vector_get_child(vector),
                        duckdb_list_type_child_type(logical_type),
                    )
                };
                let child_logical_type = CBox::new(child_logical_type, |mut v| {
                    duckdb_destroy_logical_type(&mut v)
                });
                let type_id = duckdb_get_type_id(*child_logical_type);
                let validity = duckdb_vector_get_validity(vector);
                let element_type = extract_value(
                    ptr::null_mut(),
                    0,
                    *child_logical_type,
                    type_id,
                    ptr::null(),
                    validity,
                )?;
                let mut len = if is_array {
                    duckdb_array_type_array_size(logical_type) as usize
                } else {
                    0
                };
                let value = if is_valid {
                    let range = if is_array {
                        let begin = row * len;
                        begin..(begin + len)
                    } else {
                        let entry = *(data as *const duckdb_list_entry).add(row);
                        len = entry.length as usize;
                        let begin = entry.offset as usize;
                        begin..(begin + len)
                    };
                    let data = duckdb_vector_get_data(vector);
                    Some(range.map(move |i| {
                        let element =
                            extract_value(vector, i, *child_logical_type, type_id, data, validity)?;
                        Ok(element)
                    }))
                } else {
                    None
                };
                if is_array {
                    Value::Array(
                        value.map(Iterator::collect::<Result<_>>).transpose()?,
                        element_type.into(),
                        len as u32,
                    )
                } else {
                    Value::List(
                        value.map(Iterator::collect::<Result<_>>).transpose()?,
                        element_type.into(),
                    )
                }
            }
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                let children = duckdb_struct_type_child_count(logical_type);
                let entries = (0..children)
                    .map(|i| {
                        let name = CStr::from_ptr(duckdb_struct_type_child_name(logical_type, i))
                            .to_str()
                            .map_err(|e| {
                                Error::new(e).context("While extracting field struct name")
                            })?
                            .to_owned();
                        let logical_type = duckdb_struct_type_child_type(logical_type, i);
                        let type_id = duckdb_get_type_id(logical_type);
                        let vector = duckdb_struct_vector_get_child(vector, i);
                        let data = duckdb_vector_get_data(vector);
                        let validity = duckdb_vector_get_validity(vector);
                        let value =
                            extract_value(vector, row, logical_type, type_id, data, validity)?;
                        Ok((name, value))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let value_type = entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_null()))
                    .collect();
                let value = if is_valid { Some(entries) } else { None };
                let name = CBox::new(duckdb_logical_type_get_alias(logical_type), |ptr| {
                    duckdb_free(ptr as _)
                });
                let name = if name.is_null() {
                    Default::default()
                } else {
                    CStr::from_ptr(*name).to_string_lossy()
                };
                Value::Struct(value, value_type, TableRef::new(name))
            }
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                let k_type = CBox::new(duckdb_map_type_key_type(logical_type), |mut v| {
                    duckdb_destroy_logical_type(&mut v)
                });
                let k_id = duckdb_get_type_id(*k_type);
                let v_type = CBox::new(duckdb_map_type_value_type(logical_type), |mut v| {
                    duckdb_destroy_logical_type(&mut v)
                });
                let v_id = duckdb_get_type_id(*v_type);
                // A map is a list of structs with 2 fields: "key" and "value"
                let vector = duckdb_list_vector_get_child(vector);
                let keys = duckdb_struct_vector_get_child(vector, 0);
                let vals = duckdb_struct_vector_get_child(vector, 1);
                let value = if is_valid {
                    let keys_data = duckdb_vector_get_data(keys);
                    let vals_data = duckdb_vector_get_data(vals);
                    let keys_validity = duckdb_vector_get_validity(keys);
                    let vals_validity = duckdb_vector_get_validity(vals);
                    let entry = &*(data as *const duckdb_list_entry).add(row);
                    let map = ((entry.offset as usize)..((entry.offset + entry.length) as usize))
                        .map(|i| {
                            Ok((
                                extract_value(keys, i, *k_type, k_id, keys_data, keys_validity)?,
                                extract_value(vals, i, *v_type, v_id, vals_data, vals_validity)?,
                            ))
                        })
                        .collect::<Result<_>>()?;
                    Some(map)
                } else {
                    None
                };
                let k_type = extract_value(keys, 0, *k_type, k_id, ptr::null(), ptr::null_mut())?;
                let v_type = extract_value(vals, 0, *v_type, v_id, ptr::null(), ptr::null_mut())?;
                Value::Map(value, k_type.into(), v_type.into())
            }
            DUCKDB_TYPE_DUCKDB_TYPE_UUID => Value::Uuid(if is_valid {
                let data = &*(data as *const duckdb_uhugeint).add(row);
                // TODO: remove first bit swap once this is fixed https://github.com/duckdb/duckdb-rs/issues/519
                Some(Uuid::from_u64_pair(data.upper ^ (1 << 63), data.lower))
            } else {
                None
            }),
            //  DUCKDB_TYPE_DUCKDB_TYPE_UNION =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_BIT =>
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
                let date_time = duckdb_from_timestamp(*(data as *const duckdb_timestamp).add(row));
                Value::Timestamp(if is_valid {
                    Some(time::PrimitiveDateTime::new(
                        convert_date(date_time.date)?,
                        convert_time(date_time.time)?,
                    ))
                } else {
                    None
                })
            }
            //  DUCKDB_TYPE_DUCKDB_TYPE_ANY =>
            //  DUCKDB_TYPE_DUCKDB_TYPE_VARINT =>
            DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
            _ => {
                let error = Error::msg(format!(
                    "Invalid type value: {}, must be one of the expected DUCKDB_TYPE_DUCKDB_TYPE_* variant",
                    type_id
                ));
                log::error!("{:#}", error);
                return Err(error);
            }
        };
        Ok(result)
    }
}

fn convert_interval(value: duckdb_interval) -> Interval {
    Interval::new(
        value.months as _,
        value.days as _,
        value.micros as i128 * 1_000,
    )
}
