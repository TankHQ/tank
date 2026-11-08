use crate::{
    cbox::CBox, date_to_duckdb_date, decimal_to_duckdb_decimal, i128_to_duckdb_hugeint,
    interval_to_duckdb_interval, offsetdatetime_to_duckdb_timestamp,
    primitive_date_time_to_duckdb_timestamp, time_to_duckdb_time, u128_to_duckdb_uhugeint,
    uuid_to_duckdb_hugeint,
};
use anyhow::anyhow;
use libduckdb_sys::v2::{DUCKDB_V2_LOGICAL_TYPE_ID::*, *};
use std::{
    borrow::Cow,
    cmp::{max, min},
    ptr, slice,
};
use tank_core::{CollectionType, Error, MapType, Result, StructType, TableRef, Value, ValueOwned};

#[inline(always)]
pub(crate) fn check_rc(
    f: impl FnOnce(*mut duckdb_v2_error_info_handle) -> DUCKDB_V2_ERROR,
) -> Result<()> {
    let mut err: duckdb_v2_error_info_handle = ptr::null_mut();
    let rc = f(&mut err);
    if rc == DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE {
        return Ok(());
    }
    unsafe {
        if !err.is_null() {
            let mut text = duckdb_v2_str::default();
            let msg = if duckdb_v2_error_info_get_text(err, &mut text)
                == DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE
                && !text.ptr.is_null()
            {
                String::from_utf8_lossy(slice::from_raw_parts(
                    text.ptr as *const u8,
                    text.len as usize,
                ))
                .into_owned()
            } else {
                format!("DuckDB error ({rc:?})")
            };
            duckdb_v2_error_info_destroy(&mut err);
            Err(Error::msg(msg))
        } else {
            Err(Error::msg(format!("DuckDB error ({rc:?})")))
        }
    }
}

#[inline(always)]
pub(crate) fn duckdb_str_to_str<'a>(s: duckdb_v2_str) -> &'a str {
    if s.ptr.is_null() || s.len == 0 {
        ""
    } else {
        unsafe {
            let bytes = slice::from_raw_parts(s.ptr as *const u8, s.len as usize);
            std::str::from_utf8_unchecked(bytes)
        }
    }
}

#[inline(always)]
pub(crate) fn duckdb_bytes_to_slice(b: &duckdb_v2_bytes) -> &[u8] {
    unsafe {
        let len = b.value.inlined.length;
        if len <= DUCKDB_V2_BYTES_INLINE_LENGTH {
            slice::from_raw_parts(b.value.inlined.inlined.as_ptr() as *const u8, len as usize)
        } else {
            slice::from_raw_parts(b.value.pointer.ptr as *const u8, len as usize)
        }
    }
}

pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, Value<'a>>);

impl<'a> From<&'a Value<'a>> for ValueWrap<'a> {
    #[inline(always)]
    fn from(value: &'a Value<'a>) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl<'a> From<Value<'a>> for ValueWrap<'a> {
    #[inline(always)]
    fn from(value: Value<'a>) -> Self {
        Self(Cow::Owned(value))
    }
}

impl<'a> From<ValueWrap<'a>> for Value<'a> {
    #[inline(always)]
    fn from(value: ValueWrap<'a>) -> Self {
        value.0.into_owned()
    }
}

impl ValueWrap<'_> {
    fn new_type_box() -> CBox<duckdb_v2_logical_type_handle> {
        CBox::new(ptr::null_mut(), |mut p| unsafe {
            duckdb_v2_logical_type_destroy(&mut p);
        })
    }

    pub(crate) fn new_value_box() -> CBox<duckdb_v2_value_handle> {
        CBox::new(ptr::null_mut(), |mut p| unsafe {
            duckdb_v2_value_destroy(&mut p);
        })
    }

    fn get_type_param(
        logical_type: duckdb_v2_logical_type_handle,
        index: idx_t,
    ) -> Result<(String, CBox<duckdb_v2_value_handle>)> {
        let mut name = duckdb_v2_identifier_t::default();
        let mut val = Self::new_value_box();
        unsafe {
            check_rc(|err| {
                duckdb_v2_logical_type_get_param(logical_type, index, &mut name, &mut *val, err)
            })?;
            Ok((duckdb_str_to_str(name).to_owned(), val))
        }
    }

    fn unwrap_child_type(val: duckdb_v2_value_handle) -> Result<ValueOwned> {
        unsafe {
            let mut child_ty = Self::new_type_box();
            check_rc(|err| duckdb_v2_value_get_type(val, &mut *child_ty, err))?;
            Ok(ValueWrap::try_from(*child_ty)?.into())
        }
    }

    fn create_type_value(
        factory: duckdb_v2_factory_handle,
        ty: duckdb_v2_logical_type_handle,
    ) -> Result<CBox<duckdb_v2_value_handle>> {
        let mut val = Self::new_value_box();
        unsafe {
            check_rc(|err| duckdb_v2_value_create_type(factory, ty, &mut *val, err))?;
        }
        Ok(val)
    }

    fn create_type_from_id(
        factory: duckdb_v2_factory_handle,
        id: DUCKDB_V2_LOGICAL_TYPE_ID,
        names: *const duckdb_v2_identifier_t,
        params: &[duckdb_v2_value_handle],
    ) -> Result<CBox<duckdb_v2_logical_type_handle>> {
        let mut result = Self::new_type_box();
        unsafe {
            check_rc(|err| {
                duckdb_v2_factory_create_type_from_id(
                    factory,
                    id,
                    names,
                    if params.is_empty() {
                        ptr::null()
                    } else {
                        params.as_ptr()
                    },
                    params.len() as idx_t,
                    &mut *result,
                    err,
                )
            })?;
        }
        Ok(result)
    }

    pub(crate) fn to_logical_type(
        &self,
        factory: duckdb_v2_factory_handle,
    ) -> Result<CBox<duckdb_v2_logical_type_handle>> {
        let v = self.0.as_ref();
        unsafe {
            let primitive = |id| Self::create_type_from_id(factory, id, ptr::null(), &[]);
            match v {
                Value::Null | Value::Char(..) | Value::Varchar(..) | Value::Json(..) => {
                    primitive(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)
                }
                Value::Boolean(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN),
                Value::Int8(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT),
                Value::Int16(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT),
                Value::Int32(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER),
                Value::Int64(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT),
                Value::Int128(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT),
                Value::UInt8(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT),
                Value::UInt16(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT),
                Value::UInt32(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER),
                Value::UInt64(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT),
                Value::UInt128(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT),
                Value::Float32(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT),
                Value::Float64(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE),
                Value::Blob(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_BLOB),
                Value::Date(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_DATE),
                Value::Time(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_TIME),
                Value::Timestamp(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP),
                Value::TimestampWithTimezone(..) => {
                    primitive(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ)
                }
                Value::Interval(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL),
                Value::Uuid(..) => primitive(DUCKDB_V2_LOGICAL_TYPE_ID_UUID),
                Value::Decimal(val, w, s) => {
                    let (width, scale) = match val {
                        Some(d) => {
                            let (_, w, s) = decimal_to_duckdb_decimal(d, *w, *s);
                            (w, s)
                        }
                        None if *w == 0 => {
                            let scale = min(*s, 38);
                            (max(18, scale), scale)
                        }
                        None => {
                            let scale = min(*s, 38);
                            (max(*w, scale).clamp(1, 38), scale)
                        }
                    };
                    let mut w_val = Self::new_value_box();
                    let mut s_val = Self::new_value_box();
                    check_rc(|err| {
                        duckdb_v2_value_create_utinyint(factory, width, &mut *w_val, err)
                    })?;
                    check_rc(|err| {
                        duckdb_v2_value_create_utinyint(factory, scale, &mut *s_val, err)
                    })?;
                    Self::create_type_from_id(
                        factory,
                        DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL,
                        ptr::null(),
                        &[*w_val, *s_val],
                    )
                }
                Value::List(_, t) => {
                    let elem_ty = ValueWrap::from(&t.element).to_logical_type(factory)?;
                    let elem_val = Self::create_type_value(factory, *elem_ty)?;
                    Self::create_type_from_id(
                        factory,
                        DUCKDB_V2_LOGICAL_TYPE_ID_LIST,
                        ptr::null(),
                        &[*elem_val],
                    )
                }
                Value::Array(_, t) => {
                    let elem_ty = ValueWrap::from(&t.element).to_logical_type(factory)?;
                    let elem_val = Self::create_type_value(factory, *elem_ty)?;
                    let mut len_val = Self::new_value_box();
                    check_rc(|err| {
                        duckdb_v2_value_create_bigint(factory, t.len as i64, &mut *len_val, err)
                    })?;
                    Self::create_type_from_id(
                        factory,
                        DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,
                        ptr::null(),
                        &[*elem_val, *len_val],
                    )
                }
                Value::Map(_, t) => {
                    let k_ty = ValueWrap::from(&t.key).to_logical_type(factory)?;
                    let v_ty = ValueWrap::from(&t.value).to_logical_type(factory)?;
                    let k_val = Self::create_type_value(factory, *k_ty)?;
                    let v_val = Self::create_type_value(factory, *v_ty)?;
                    Self::create_type_from_id(
                        factory,
                        DUCKDB_V2_LOGICAL_TYPE_ID_MAP,
                        ptr::null(),
                        &[*k_val, *v_val],
                    )
                }
                Value::Struct(_, t) => {
                    let names = t
                        .fields
                        .iter()
                        .map(|(n, _)| duckdb_v2_str::from(n.as_str()))
                        .collect::<Vec<_>>();
                    let mut type_vals = Vec::with_capacity(t.fields.len());
                    for (_, field_v) in &t.fields {
                        let f_ty = ValueWrap::from(field_v).to_logical_type(factory)?;
                        type_vals.push(Self::create_type_value(factory, *f_ty)?);
                    }
                    let raw_vals = type_vals.iter().map(|v| **v).collect::<Vec<_>>();
                    let base = Self::create_type_from_id(
                        factory,
                        DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
                        if names.is_empty() {
                            ptr::null()
                        } else {
                            names.as_ptr()
                        },
                        &raw_vals,
                    )?;
                    if t.name.name.is_empty() {
                        Ok(base)
                    } else {
                        let alias = duckdb_v2_str::from(t.name.name.as_ref());
                        let mut aliased = Self::new_type_box();
                        check_rc(|err| {
                            duckdb_v2_factory_create_type_with_alias(
                                factory,
                                *base,
                                &alias,
                                &mut *aliased,
                                err,
                            )
                        })?;
                        Ok(aliased)
                    }
                }
                _ => Err(anyhow!("Value `{v:?}` is unsupported by DuckDB")),
            }
        }
    }

    pub(crate) fn to_duckdb_value(
        &self,
        factory: duckdb_v2_factory_handle,
    ) -> Result<CBox<duckdb_v2_value_handle>> {
        let value = self.0.as_ref();
        unsafe {
            if value.is_null() {
                let ty = self.to_logical_type(factory)?;
                let mut out = Self::new_value_box();
                check_rc(|err| duckdb_v2_value_create_null(factory, *ty, &mut *out, err))?;
                return Ok(out);
            }
            let mut out = Self::new_value_box();
            match value {
                Value::Boolean(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_bool(factory, *v, &mut *out, err))?;
                }
                Value::Int8(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_tinyint(factory, *v, &mut *out, err))?;
                }
                Value::Int16(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_smallint(factory, *v, &mut *out, err))?;
                }
                Value::Int32(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_int(factory, *v, &mut *out, err))?;
                }
                Value::Int64(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_bigint(factory, *v, &mut *out, err))?;
                }
                Value::Int128(Some(v)) => {
                    let raw = i128_to_duckdb_hugeint(**v);
                    check_rc(|err| duckdb_v2_value_create_hugeint(factory, &raw, &mut *out, err))?;
                }
                Value::UInt8(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_utinyint(factory, *v, &mut *out, err))?;
                }
                Value::UInt16(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_usmallint(factory, *v, &mut *out, err))?;
                }
                Value::UInt32(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_uint(factory, *v, &mut *out, err))?;
                }
                Value::UInt64(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_ubigint(factory, *v, &mut *out, err))?;
                }
                Value::UInt128(Some(v)) => {
                    let raw = u128_to_duckdb_uhugeint(**v);
                    check_rc(|err| {
                        duckdb_v2_value_create_uhugeint(factory, &raw, &mut *out, err)
                    })?;
                }
                Value::Float32(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_float(factory, *v, &mut *out, err))?;
                }
                Value::Float64(Some(v)) => {
                    check_rc(|err| duckdb_v2_value_create_double(factory, *v, &mut *out, err))?;
                }
                Value::Decimal(Some(v), w, s) => {
                    let (raw, width, scale) = decimal_to_duckdb_decimal(v, *w, *s);
                    check_rc(|err| {
                        duckdb_v2_value_create_decimal(factory, &raw, width, scale, &mut *out, err)
                    })?;
                }
                Value::Char(Some(v)) => {
                    let mut buf = [0u8; 4];
                    let s = v.encode_utf8(&mut buf);
                    let raw = duckdb_v2_str::from(&*s);
                    check_rc(|err| duckdb_v2_value_create_varchar(factory, &raw, &mut *out, err))?;
                }
                Value::Varchar(Some(v)) => {
                    let raw = duckdb_v2_str::from(v.as_ref());
                    check_rc(|err| duckdb_v2_value_create_varchar(factory, &raw, &mut *out, err))?;
                }
                Value::Blob(Some(v)) => {
                    let raw = duckdb_v2_str {
                        ptr: if v.is_empty() {
                            ptr::null()
                        } else {
                            v.as_ptr() as *const _
                        },
                        len: v.len() as idx_t,
                    };
                    check_rc(|err| duckdb_v2_value_create_blob(factory, &raw, &mut *out, err))?;
                }
                Value::Date(Some(v)) => {
                    let raw = date_to_duckdb_date(v);
                    check_rc(|err| duckdb_v2_value_create_date(factory, raw, &mut *out, err))?;
                }
                Value::Time(Some(v)) => {
                    let raw = time_to_duckdb_time(v);
                    check_rc(|err| duckdb_v2_value_create_time(factory, raw, &mut *out, err))?;
                }
                Value::Timestamp(Some(v)) => {
                    let raw = primitive_date_time_to_duckdb_timestamp(v);
                    check_rc(|err| {
                        duckdb_v2_value_create_timestamp(factory, raw, &mut *out, err)
                    })?;
                }
                Value::TimestampWithTimezone(Some(v)) => {
                    let raw = offsetdatetime_to_duckdb_timestamp(v);
                    check_rc(|err| {
                        duckdb_v2_value_create_timestamp_tz(factory, raw, &mut *out, err)
                    })?;
                }
                Value::Interval(Some(v)) => {
                    let raw = interval_to_duckdb_interval(v);
                    check_rc(|err| {
                        duckdb_v2_value_create_interval(factory, &raw, &mut *out, err)
                    })?;
                }
                Value::Uuid(Some(v)) => {
                    let raw = uuid_to_duckdb_hugeint(v);
                    check_rc(|err| duckdb_v2_value_create_uuid(factory, &raw, &mut *out, err))?;
                }
                Value::Array(Some(v), t) | Value::List(Some(v), t) => {
                    let ty = ValueWrap::from(&t.element).to_logical_type(factory)?;
                    let vals = v
                        .iter()
                        .map(|el| ValueWrap::from(el).to_duckdb_value(factory))
                        .collect::<Result<Vec<_>>>()?;
                    let raw_vals = vals.iter().map(|el| **el).collect::<Vec<_>>();
                    let ptr = if raw_vals.is_empty() {
                        ptr::null()
                    } else {
                        raw_vals.as_ptr()
                    };
                    if matches!(value, Value::Array(..)) {
                        check_rc(|err| {
                            duckdb_v2_value_create_array(
                                factory,
                                *ty,
                                ptr,
                                raw_vals.len() as idx_t,
                                &mut *out,
                                err,
                            )
                        })?;
                    } else {
                        check_rc(|err| {
                            duckdb_v2_value_create_list(
                                factory,
                                *ty,
                                ptr,
                                raw_vals.len() as idx_t,
                                &mut *out,
                                err,
                            )
                        })?;
                    }
                }
                Value::Map(Some(v), t) => {
                    let k_ty = ValueWrap::from(&t.key).to_logical_type(factory)?;
                    let v_ty = ValueWrap::from(&t.value).to_logical_type(factory)?;
                    let keys = v
                        .keys()
                        .map(|k| ValueWrap::from(k).to_duckdb_value(factory))
                        .collect::<Result<Vec<_>>>()?;
                    let vals = v
                        .values()
                        .map(|val| ValueWrap::from(val).to_duckdb_value(factory))
                        .collect::<Result<Vec<_>>>()?;
                    let raw_keys = keys.iter().map(|k| **k).collect::<Vec<_>>();
                    let raw_vals = vals.iter().map(|val| **val).collect::<Vec<_>>();
                    check_rc(|err| {
                        duckdb_v2_value_create_map(
                            factory,
                            *k_ty,
                            *v_ty,
                            if raw_keys.is_empty() {
                                ptr::null()
                            } else {
                                raw_keys.as_ptr()
                            },
                            if raw_vals.is_empty() {
                                ptr::null()
                            } else {
                                raw_vals.as_ptr()
                            },
                            v.len() as idx_t,
                            &mut *out,
                            err,
                        )
                    })?;
                }
                Value::Struct(Some(v), t) => {
                    let names = v
                        .iter()
                        .map(|(n, _)| duckdb_v2_str::from(n.as_str()))
                        .collect::<Vec<_>>();
                    let vals = v
                        .iter()
                        .map(|(_, val)| ValueWrap::from(val).to_duckdb_value(factory))
                        .collect::<Result<Vec<_>>>()?;
                    let raw_vals = vals.iter().map(|val| **val).collect::<Vec<_>>();
                    check_rc(|err| {
                        duckdb_v2_value_create_struct(
                            factory,
                            if names.is_empty() {
                                ptr::null()
                            } else {
                                names.as_ptr()
                            },
                            if raw_vals.is_empty() {
                                ptr::null()
                            } else {
                                raw_vals.as_ptr()
                            },
                            v.len() as idx_t,
                            &mut *out,
                            err,
                        )
                    })?;
                    if !t.name.name.is_empty() {
                        let aliased_ty = self.to_logical_type(factory)?;
                        let mut casted = Self::new_value_box();
                        check_rc(|err| {
                            duckdb_v2_value_cast(factory, *out, *aliased_ty, &mut *casted, err)
                        })?;
                        out = casted;
                    }
                }
                _ => return Err(anyhow!("Cannot use a {value:?} as a query parameter")),
            }
            Ok(out)
        }
    }
}

impl TryFrom<duckdb_v2_logical_type_handle> for ValueWrap<'static> {
    type Error = Error;

    fn try_from(logical_type: duckdb_v2_logical_type_handle) -> Result<Self> {
        unsafe {
            let mut id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
            check_rc(|err| duckdb_v2_logical_type_get_id(logical_type, &mut id, err))?;
            let value = match id {
                DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL => Value::Null,
                DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN => Value::Boolean(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT => Value::Int8(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT => Value::Int16(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER => Value::Int32(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT => Value::Int64(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT => Value::Int128(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT => Value::UInt8(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT => Value::UInt16(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER => Value::UInt32(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT => Value::UInt64(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT => Value::UInt128(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT => Value::Float32(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE => Value::Float64(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_DATE => Value::Date(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_TIME
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ => Value::Time(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ
                | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS => Value::Timestamp(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL => Value::Interval(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR => Value::Varchar(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_ENUM => Value::Unknown(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_BLOB => Value::Blob(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_UUID => Value::Uuid(None),
                DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL => {
                    let (_, w_val) = Self::get_type_param(logical_type, 0)?;
                    let (_, s_val) = Self::get_type_param(logical_type, 1)?;
                    let mut width = 0;
                    let mut scale = 0;
                    check_rc(|err| duckdb_v2_value_get_utinyint(*w_val, &mut width, err))?;
                    check_rc(|err| duckdb_v2_value_get_utinyint(*s_val, &mut scale, err))?;
                    Value::Decimal(None, width, scale)
                }
                DUCKDB_V2_LOGICAL_TYPE_ID_LIST => {
                    let (_, elem_val) = Self::get_type_param(logical_type, 0)?;
                    let element = Self::unwrap_child_type(*elem_val)?;
                    Value::List(None, Box::new(CollectionType { element, len: 0 }))
                }
                DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY => {
                    let (_, elem_val) = Self::get_type_param(logical_type, 0)?;
                    let (_, len_val) = Self::get_type_param(logical_type, 1)?;
                    let element = Self::unwrap_child_type(*elem_val)?;
                    let mut len = 0_i64;
                    check_rc(|err| duckdb_v2_value_get_bigint(*len_val, &mut len, err))?;
                    Value::Array(
                        None,
                        Box::new(CollectionType {
                            element,
                            len: len as u32,
                        }),
                    )
                }
                DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT | DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE => {
                    let mut count = 0;
                    check_rc(|err| {
                        duckdb_v2_logical_type_get_param_count(logical_type, &mut count, err)
                    })?;
                    let mut fields = Vec::with_capacity(count as usize);
                    for i in 0..count {
                        let (name, val) = Self::get_type_param(logical_type, i)?;
                        fields.push((name, Self::unwrap_child_type(*val)?));
                    }
                    let mut type_name = duckdb_v2_identifier_t::default();
                    check_rc(|err| {
                        duckdb_v2_logical_type_get_name(logical_type, &mut type_name, err)
                    })?;
                    let raw_name = duckdb_str_to_str(type_name);
                    let alias = if raw_name == "STRUCT" || raw_name == "TUPLE" {
                        ""
                    } else {
                        raw_name
                    };
                    Value::Struct(
                        None,
                        Box::new(StructType {
                            fields,
                            name: TableRef::new(alias.to_owned().into()),
                        }),
                    )
                }
                DUCKDB_V2_LOGICAL_TYPE_ID_MAP => {
                    let (_, k_val) = Self::get_type_param(logical_type, 0)?;
                    let (_, v_val) = Self::get_type_param(logical_type, 1)?;
                    Value::Map(
                        None,
                        Box::new(MapType {
                            key: Self::unwrap_child_type(*k_val)?,
                            value: Self::unwrap_child_type(*v_val)?,
                        }),
                    )
                }
                _ => return Err(anyhow!("Unsupported DuckDB logical type: {id:?}")),
            };
            Ok(Self(Cow::Owned(value)))
        }
    }
}
