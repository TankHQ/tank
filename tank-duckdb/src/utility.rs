use crate::{
    cbox::CBox, date_to_duckdb_date, decimal_to_duckdb_decimal, i128_to_duckdb_hugeint,
    interval_to_duckdb_interval, offsetdatetime_to_duckdb_timestamp,
    primitive_date_time_to_duckdb_timestamp, time_to_duckdb_time, u128_to_duckdb_uhugeint,
};
use libduckdb_sys::*;
use std::{ffi::c_char, ptr};
use tank_core::{Value, as_c_string};

pub(crate) fn tank_value_to_duckdb_logical_type(v: &Value) -> CBox<duckdb_logical_type> {
    unsafe {
        let mut result = CBox::new(ptr::null_mut(), |mut p| duckdb_destroy_logical_type(&mut p));
        match v {
            Value::Null => *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL),
            Value::Boolean(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN)
            }
            Value::Int8(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_TINYINT)
            }
            Value::Int16(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT)
            }
            Value::Int32(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER)
            }
            Value::Int64(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT)
            }
            Value::Int128(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT)
            }
            Value::UInt8(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT)
            }
            Value::UInt16(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT)
            }
            Value::UInt32(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER)
            }
            Value::UInt64(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT)
            }
            Value::UInt128(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT)
            }
            Value::Float32(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_FLOAT)
            }
            Value::Float64(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE)
            }
            Value::Decimal(.., w, s) => *result = duckdb_create_decimal_type(*w, *s),
            Value::Char(..) | Value::Varchar(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)
            }
            Value::Blob(..) => *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BLOB),
            Value::Date(..) => *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DATE),
            Value::Time(..) => *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_TIME),
            Value::Timestamp(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP)
            }
            Value::TimestampWithTimezone(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ)
            }
            Value::Interval(..) => {
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL)
            }
            Value::Uuid(..) => *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UUID),
            Value::Array(.., t, s) => {
                *result = duckdb_create_array_type(*tank_value_to_duckdb_logical_type(t), *s as u64)
            }
            Value::List(.., t) => {
                *result = duckdb_create_list_type(*tank_value_to_duckdb_logical_type(t))
            }
            Value::Map(.., k, v) => {
                *result = duckdb_create_map_type(
                    *tank_value_to_duckdb_logical_type(k),
                    *tank_value_to_duckdb_logical_type(v),
                )
            }
            Value::Struct(.., v, type_ref) => {
                let names = v
                    .iter()
                    .map(|name| as_c_string(name.0.to_string()))
                    .collect::<Vec<_>>();
                let types = v
                    .iter()
                    .map(|v| tank_value_to_duckdb_logical_type(&v.1))
                    .collect::<Vec<_>>();
                *result = duckdb_create_struct_type(
                    types.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                    names
                        .iter()
                        .map(|n| n.as_ptr())
                        .collect::<Vec<_>>()
                        .as_mut_ptr(),
                    v.len() as u64,
                );
                let name = as_c_string(type_ref.name());
                duckdb_logical_type_set_alias(*result, name.as_ptr());
            }
            _ => {
                log::error!("tank::Value `{:?}` is unsupported", v);
                *result = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_INVALID)
            }
        }
        result
    }
}

pub(crate) fn tank_value_to_duckdb_value(value: &Value) -> CBox<duckdb_value> {
    unsafe {
        CBox::new(
            match value {
                v if v.is_null() => duckdb_create_null_value(),
                Value::Boolean(Some(v)) => duckdb_create_bool(*v),
                Value::Int8(Some(v)) => duckdb_create_int8(*v),
                Value::Int16(Some(v)) => duckdb_create_int16(*v),
                Value::Int32(Some(v)) => duckdb_create_int32(*v),
                Value::Int64(Some(v)) => duckdb_create_int64(*v),
                Value::Int128(Some(v)) => duckdb_create_hugeint(i128_to_duckdb_hugeint(*v)),
                Value::UInt8(Some(v)) => duckdb_create_uint8(*v),
                Value::UInt16(Some(v)) => duckdb_create_uint16(*v),
                Value::UInt32(Some(v)) => duckdb_create_uint32(*v),
                Value::UInt64(Some(v)) => duckdb_create_uint64(*v),
                Value::UInt128(Some(v)) => duckdb_create_uhugeint(u128_to_duckdb_uhugeint(*v)),
                Value::Float32(Some(v)) => duckdb_create_float(*v),
                Value::Float64(Some(v)) => duckdb_create_double(*v),
                Value::Decimal(Some(v), w, s) => {
                    duckdb_create_decimal(decimal_to_duckdb_decimal(v, *w, *s))
                }
                Value::Char(Some(v)) => {
                    duckdb_create_varchar_length(as_c_string(v.to_string()).as_ptr(), 1)
                }
                Value::Varchar(Some(v)) => duckdb_create_varchar_length(
                    as_c_string(v.to_string()).as_ptr(),
                    v.len() as u64,
                ),
                Value::Blob(Some(v)) => duckdb_create_blob(v.as_ptr(), v.len() as u64),
                Value::Date(Some(v)) => duckdb_create_date(date_to_duckdb_date(v)),
                Value::Time(Some(v)) => duckdb_create_time(time_to_duckdb_time(v)),
                Value::Timestamp(Some(v)) => {
                    duckdb_create_timestamp(primitive_date_time_to_duckdb_timestamp(v))
                }
                Value::TimestampWithTimezone(Some(v)) => {
                    duckdb_create_timestamp_tz(offsetdatetime_to_duckdb_timestamp(v))
                }
                Value::Interval(Some(v)) => duckdb_create_interval(interval_to_duckdb_interval(v)),
                Value::Uuid(Some(v)) => duckdb_create_uuid(u128_to_duckdb_uhugeint(v.as_u128())),
                Value::Array(Some(v), ty, len) => {
                    let values = v
                        .iter()
                        .map(|v| tank_value_to_duckdb_value(v))
                        .collect::<Vec<_>>();
                    duckdb_create_array_value(
                        *tank_value_to_duckdb_logical_type(ty),
                        values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                        *len as u64,
                    )
                }
                Value::List(Some(v), ty) => {
                    let values = v
                        .iter()
                        .map(|v| tank_value_to_duckdb_value(v))
                        .collect::<Vec<_>>();
                    duckdb_create_list_value(
                        *tank_value_to_duckdb_logical_type(ty),
                        values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                        values.len() as u64,
                    )
                }
                Value::Map(Some(v), ..) => {
                    let keys = v.keys().map(tank_value_to_duckdb_value).collect::<Vec<_>>();
                    let values = v
                        .values()
                        .map(tank_value_to_duckdb_value)
                        .collect::<Vec<_>>();
                    duckdb_create_map_value(
                        *tank_value_to_duckdb_logical_type(value),
                        keys.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                        values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                        v.len() as u64,
                    )
                }
                Value::Struct(Some(v), ..) => {
                    let values = v
                        .iter()
                        .map(|v| tank_value_to_duckdb_value(&v.1))
                        .collect::<Vec<_>>();
                    duckdb_create_struct_value(
                        *tank_value_to_duckdb_logical_type(value),
                        values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                    )
                }
                _ => {
                    log::error!("{:?} is unsupported", value);
                    duckdb_create_varchar("".as_ptr() as *const c_char)
                }
            },
            |mut p| duckdb_destroy_value(&mut p),
        )
    }
}
