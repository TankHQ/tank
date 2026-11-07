use crate::{
    ValueWrap, bytes_from_v2, cbox::CBox, check_rc, duckdb_date_to_date, duckdb_hugeint_to_i128,
    duckdb_hugeint_to_uuid, duckdb_interval_to_interval, duckdb_time_ns_to_time,
    duckdb_time_to_time, duckdb_uhugeint_to_u128, nanos_to_primitive_date_time, str_from_v2,
};
use anyhow::anyhow;
use libduckdb_sys::v2::{DUCKDB_V2_LOGICAL_TYPE_ID::*, *};
use std::{collections::HashMap, ffi::c_void, mem, ptr};
use tank_core::{Result, Value, ValueOwned, decimal_from_scaled};

pub(crate) struct VectorNode {
    vector: duckdb_v2_vector_handle,
    id: DUCKDB_V2_LOGICAL_TYPE_ID,
    data: *const c_void,
    validity: *const u64,
    children: Box<[VectorNode]>,
}

unsafe impl Send for VectorNode {}
unsafe impl Sync for VectorNode {}

impl VectorNode {
    unsafe fn ensure_flat(vector: duckdb_v2_vector_handle) -> Result<()> {
        unsafe {
            let mut vt = DUCKDB_V2_VECTOR_TYPE::DUCKDB_V2_VECTOR_TYPE_OTHER;
            check_rc(|err| duckdb_v2_vector_get_vector_type(vector, &mut vt, err))?;
            if vt != DUCKDB_V2_VECTOR_TYPE::DUCKDB_V2_VECTOR_TYPE_FLAT {
                check_rc(|err| duckdb_v2_vector_flatten(vector, err))?;
            }
            Ok(())
        }
    }

    pub(crate) unsafe fn from_vector(
        vector: duckdb_v2_vector_handle,
        ty: &ValueOwned,
    ) -> Result<Self> {
        unsafe {
            Self::ensure_flat(vector)?;
            let mut view: duckdb_v2_vector_view = mem::zeroed();
            check_rc(|err| duckdb_v2_vector_get_view(vector, &mut view, err))?;
            let id = if matches!(ty, Value::Time(..) | Value::Timestamp(..) | Value::Varchar(..)) {
                let mut vec_ty = CBox::new(ptr::null_mut(), |mut p| {
                    duckdb_v2_logical_type_destroy(&mut p);
                });
                check_rc(|err| duckdb_v2_vector_get_logical_type(vector, &mut *vec_ty, err))?;
                let mut id = DUCKDB_V2_LOGICAL_TYPE_ID::DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
                check_rc(|err| duckdb_v2_logical_type_get_id(*vec_ty, &mut id, err))?;
                id
            } else {
                DUCKDB_V2_LOGICAL_TYPE_ID::DUCKDB_V2_LOGICAL_TYPE_ID_INVALID
            };
            let children = match ty {
                Value::List(_, t) | Value::Array(_, t) => {
                    let mut child = ptr::null_mut();
                    check_rc(|err| duckdb_v2_vector_get_child(vector, 0, &mut child, err))?;
                    Box::new([Self::from_vector(child, &t.element)?])
                }
                Value::Struct(_, t) => t
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, f_ty))| {
                        let mut child = ptr::null_mut();
                        check_rc(|err| {
                            duckdb_v2_vector_get_child(vector, i as idx_t, &mut child, err)
                        })?;
                        Self::from_vector(child, f_ty)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_boxed_slice(),
                Value::Map(_, t) => {
                    let mut entries = ptr::null_mut();
                    check_rc(|err| duckdb_v2_vector_get_child(vector, 0, &mut entries, err))?;
                    Self::ensure_flat(entries)?;
                    let mut keys = ptr::null_mut();
                    let mut vals = ptr::null_mut();
                    check_rc(|err| duckdb_v2_vector_get_child(entries, 0, &mut keys, err))?;
                    check_rc(|err| duckdb_v2_vector_get_child(entries, 1, &mut vals, err))?;
                    Box::new([
                        Self::from_vector(keys, &t.key)?,
                        Self::from_vector(vals, &t.value)?,
                    ])
                }
                _ => Box::new([]),
            };
            Ok(Self {
                vector,
                id,
                data: view.data,
                validity: view.validity,
                children,
            })
        }
    }

    #[inline(always)]
    unsafe fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null()
            || ((unsafe { *self.validity.add(row >> 6) } >> (row & 63)) & 1) != 0
    }

    pub(crate) fn extract(&self, row: usize, ty: &ValueOwned) -> Result<ValueOwned> {
        unsafe {
            let has_data =
                !self.data.is_null() || matches!(ty, Value::Array(..) | Value::Struct(..));
            if !has_data || !self.is_valid(row) {
                return Ok(ty.clone());
            }
            let data = self.data;
            let value = match ty {
                Value::Null => Value::Null,
                Value::Boolean(..) => Value::Boolean(Some(*(data as *const bool).add(row))),
                Value::Int8(..) => Value::Int8(Some(*(data as *const i8).add(row))),
                Value::Int16(..) => Value::Int16(Some(*(data as *const i16).add(row))),
                Value::Int32(..) => Value::Int32(Some(*(data as *const i32).add(row))),
                Value::Int64(..) => Value::Int64(Some(*(data as *const i64).add(row))),
                Value::Int128(..) => {
                    let raw = &*(data as *const duckdb_v2_hugeint_t).add(row);
                    Value::Int128(Some(duckdb_hugeint_to_i128(raw).into()))
                }
                Value::UInt8(..) => Value::UInt8(Some(*(data as *const u8).add(row))),
                Value::UInt16(..) => Value::UInt16(Some(*(data as *const u16).add(row))),
                Value::UInt32(..) => Value::UInt32(Some(*(data as *const u32).add(row))),
                Value::UInt64(..) => Value::UInt64(Some(*(data as *const u64).add(row))),
                Value::UInt128(..) => {
                    let raw = &*(data as *const duckdb_v2_uhugeint_t).add(row);
                    Value::UInt128(Some(duckdb_uhugeint_to_u128(raw).into()))
                }
                Value::Float32(..) => Value::Float32(Some(*(data as *const f32).add(row))),
                Value::Float64(..) => Value::Float64(Some(*(data as *const f64).add(row))),
                Value::Decimal(_, width, scale) => {
                    let num = if *width <= 4 {
                        *(data as *const i16).add(row) as i128
                    } else if *width <= 9 {
                        *(data as *const i32).add(row) as i128
                    } else if *width <= 18 {
                        *(data as *const i64).add(row) as i128
                    } else {
                        duckdb_hugeint_to_i128(&*(data as *const duckdb_v2_hugeint_t).add(row))
                    };
                    Value::Decimal(Some(decimal_from_scaled(num, *scale as u32)?), *width, *scale)
                }
                Value::Date(..) => {
                    let days = *(data as *const i32).add(row);
                    Value::Date(Some(duckdb_date_to_date(days)?))
                }
                Value::Time(..) => match self.id {
                    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS => {
                        let nanos = *(data as *const i64).add(row);
                        Value::Time(Some(duckdb_time_ns_to_time(nanos)?))
                    }
                    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ => {
                        let bits = *(data as *const u64).add(row);
                        Value::Time(Some(duckdb_time_to_time((bits >> 24) as i64)?))
                    }
                    _ => {
                        let micros = *(data as *const i64).add(row);
                        Value::Time(Some(duckdb_time_to_time(micros)?))
                    }
                },
                Value::Timestamp(..) => {
                    let raw = *(data as *const i64).add(row) as i128;
                    let nanos = match self.id {
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC => raw * 1_000_000_000,
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS => raw * 1_000_000,
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS
                        | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS => raw,
                        _ => raw * 1_000,
                    };
                    Value::Timestamp(Some(nanos_to_primitive_date_time(nanos)?))
                }
                Value::Interval(..) => {
                    let raw = *(data as *const duckdb_v2_interval_t).add(row);
                    Value::Interval(Some(duckdb_interval_to_interval(raw)))
                }
                Value::Varchar(..) => {
                    if self.id == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM {
                        let mut val = ValueWrap::new_value_box();
                        check_rc(|err| {
                            duckdb_v2_vector_get_value(self.vector, row as idx_t, &mut *val, err)
                        })?;
                        let mut s = duckdb_v2_str::default();
                        check_rc(|err| duckdb_v2_value_get_varchar(*val, &mut s, err))?;
                        Value::Varchar(Some(str_from_v2(s).to_owned().into()))
                    } else {
                        let bytes = bytes_from_v2(&*(data as *const duckdb_v2_bytes).add(row));
                        Value::Varchar(Some(String::from_utf8_lossy(bytes).into_owned().into()))
                    }
                }
                Value::Blob(..) => {
                    let bytes = bytes_from_v2(&*(data as *const duckdb_v2_bytes).add(row));
                    Value::Blob(Some(bytes.to_vec().into()))
                }
                Value::Uuid(..) => {
                    let raw = &*(data as *const duckdb_v2_hugeint_t).add(row);
                    Value::Uuid(Some(duckdb_hugeint_to_uuid(raw)))
                }
                Value::List(_, collection_type) => {
                    let entry = *(data as *const duckdb_v2_list_entry).add(row);
                    let begin = entry.offset as usize;
                    let end = begin + entry.length as usize;
                    let child = &self.children[0];
                    let values = (begin..end)
                        .map(|i| child.extract(i, &collection_type.element))
                        .collect::<Result<Vec<_>>>()?
                        .into_boxed_slice();
                    Value::List(Some(values), collection_type.clone())
                }
                Value::Array(_, collection_type) => {
                    let len = collection_type.len as usize;
                    let begin = row * len;
                    let end = begin + len;
                    let child = &self.children[0];
                    let values = (begin..end)
                        .map(|i| child.extract(i, &collection_type.element))
                        .collect::<Result<Vec<_>>>()?
                        .into_boxed_slice();
                    Value::Array(Some(values), collection_type.clone())
                }
                Value::Struct(_, struct_type) => {
                    let entries = struct_type
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, (name, f_ty))| {
                            Ok((name.clone(), self.children[i].extract(row, f_ty)?))
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_boxed_slice();
                    Value::Struct(Some(entries), struct_type.clone())
                }
                Value::Map(_, map_type) => {
                    let entry = *(data as *const duckdb_v2_list_entry).add(row);
                    let begin = entry.offset as usize;
                    let end = begin + entry.length as usize;
                    let keys_node = &self.children[0];
                    let vals_node = &self.children[1];
                    let map = (begin..end)
                        .map(|i| {
                            Ok((
                                keys_node.extract(i, &map_type.key)?,
                                vals_node.extract(i, &map_type.value)?,
                            ))
                        })
                        .collect::<Result<HashMap<_, _>>>()?;
                    Value::Map(Some(Box::new(map)), map_type.clone())
                }
                _ => return Err(anyhow!("Unsupported DuckDB column type: {ty:?}")),
            };
            Ok(value)
        }
    }
}
