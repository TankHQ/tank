use crate::cbox::CBox;
use anyhow::anyhow;
use libduckdb_sys::*;
use std::ffi::CStr;
use tank_core::{CollectionType, MapType, Result, StructType, TableRef, Value, ValueOwned};

type TypeBox = CBox<duckdb_logical_type>;

fn take_string(ptr: *mut ::std::os::raw::c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe {
        let text = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        duckdb_free(ptr as _);
        text
    }
}

fn own(logical_type: duckdb_logical_type) -> TypeBox {
    CBox::new(logical_type, |mut p| unsafe {
        duckdb_destroy_logical_type(&mut p)
    })
}

/// Builds an empty value describing the layout of `logical_type`, used as the schema of a column.
pub(crate) fn type_to_value(logical_type: duckdb_logical_type) -> Result<ValueOwned> {
    unsafe {
        Ok(match duckdb_get_type_id(logical_type) {
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
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Value::Varchar(None),
            DUCKDB_TYPE_DUCKDB_TYPE_ENUM => Value::Unknown(None),
            DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Value::Blob(None),
            DUCKDB_TYPE_DUCKDB_TYPE_UUID => Value::Uuid(None),
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Value::Decimal(
                None,
                duckdb_decimal_width(logical_type),
                duckdb_decimal_scale(logical_type),
            ),
            DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
                let element = own(duckdb_list_type_child_type(logical_type));
                Value::List(
                    None,
                    Box::new(CollectionType {
                        element: type_to_value(*element)?,
                        len: 0,
                    }),
                )
            }
            DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                let element = own(duckdb_array_type_child_type(logical_type));
                Value::Array(
                    None,
                    Box::new(CollectionType {
                        element: type_to_value(*element)?,
                        len: duckdb_array_type_array_size(logical_type) as u32,
                    }),
                )
            }
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                let count = duckdb_struct_type_child_count(logical_type);
                let mut fields = Vec::with_capacity(count as usize);
                for i in 0..count {
                    let name = take_string(duckdb_struct_type_child_name(logical_type, i));
                    let child = own(duckdb_struct_type_child_type(logical_type, i));
                    fields.push((name, type_to_value(*child)?));
                }
                let name = take_string(duckdb_logical_type_get_alias(logical_type));
                Value::Struct(
                    None,
                    Box::new(StructType {
                        fields,
                        name: TableRef::new(name.into()),
                    }),
                )
            }
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                let key = own(duckdb_map_type_key_type(logical_type));
                let value = own(duckdb_map_type_value_type(logical_type));
                Value::Map(
                    None,
                    Box::new(MapType {
                        key: type_to_value(*key)?,
                        value: type_to_value(*value)?,
                    }),
                )
            }
            id => return Err(anyhow!("Unsupported DuckDB logical type: {id}")),
        })
    }
}
