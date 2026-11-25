use anyhow::anyhow;
use libduckdb_sys::*;
use rust_decimal::Decimal;
use std::{
    cmp::{max, min},
    ffi::c_char,
    hint,
    ops::{Deref, DerefMut},
    ptr,
};
use tank_core::{Error, Interval, Result, Value, as_c_string};
use time::{PrimitiveDateTime, Time};

pub(crate) trait CResource: Sized {
    fn is_null(&self) -> bool;
    fn finalize(&mut self);
}

macro_rules! impl_resource {
    ($($ty:ty => $destroy:ident),* $(,)?) => {$(
        impl CResource for $ty {
            #[inline(always)]
            fn is_null(&self) -> bool {
                (*self).is_null()
            }
            #[inline(always)]
            fn finalize(&mut self) {
                unsafe { $destroy(self) };
            }
        }
    )*};
}

impl_resource! {
    duckdb_appender => duckdb_appender_destroy,
    duckdb_config => duckdb_destroy_config,
    duckdb_connection => duckdb_disconnect,
    duckdb_data_chunk => duckdb_destroy_data_chunk,
    duckdb_database => duckdb_close,
    duckdb_extracted_statements => duckdb_destroy_extracted,
    duckdb_instance_cache => duckdb_destroy_instance_cache,
    duckdb_logical_type => duckdb_destroy_logical_type,
    duckdb_prepared_statement => duckdb_destroy_prepare,
    duckdb_value => duckdb_destroy_value,
}

impl CResource for *mut c_char {
    #[inline(always)]
    fn is_null(&self) -> bool {
        (*self).is_null()
    }
    #[inline(always)]
    fn finalize(&mut self) {
        unsafe { duckdb_free(*self as _) };
    }
}

impl CResource for duckdb_result {
    #[inline(always)]
    fn is_null(&self) -> bool {
        false
    }
    #[inline(always)]
    fn finalize(&mut self) {
        unsafe { duckdb_destroy_result(self) };
    }
}

/// Owned RAII wrapper around a DuckDB C handle or result struct.
#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct CBox<T: CResource>(pub(crate) T);

impl<T> CBox<*mut T>
where
    *mut T: CResource,
{
    #[inline(always)]
    pub(crate) const fn null() -> Self {
        Self(ptr::null_mut())
    }
}

impl<T: CResource> Deref for CBox<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: CResource> DerefMut for CBox<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl<T: CResource> Send for CBox<T> {}
unsafe impl<T: CResource> Sync for CBox<T> {}

impl<T: CResource> Drop for CBox<T> {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.0.is_null() {
            self.0.finalize();
        }
    }
}

impl From<&Value<'_>> for CBox<duckdb_logical_type> {
    fn from(value: &Value<'_>) -> Self {
        unsafe {
            CBox(match value {
                Value::Decimal(_, w, s) => duckdb_create_decimal_type(*w, *s),
                Value::Array(_, t) => duckdb_create_array_type(*Self::from(&t.element), t.len as _),
                Value::List(_, t) => duckdb_create_list_type(*Self::from(&t.element)),
                Value::Map(_, t) => {
                    duckdb_create_map_type(*Self::from(&t.key), *Self::from(&t.value))
                }
                Value::Struct(_, t) => {
                    let names = t
                        .fields
                        .iter()
                        .map(|(n, _)| as_c_string(n))
                        .collect::<Box<[_]>>();
                    let mut raw_names = names.iter().map(|n| n.as_ptr()).collect::<Box<[_]>>();
                    let mut types = t
                        .fields
                        .iter()
                        .map(|(_, v)| Self::from(v))
                        .collect::<Box<[_]>>();
                    let ptr = duckdb_create_struct_type(
                        types.as_mut_ptr() as _,
                        raw_names.as_mut_ptr(),
                        t.fields.len() as _,
                    );
                    duckdb_logical_type_set_alias(ptr, as_c_string(t.name.name.as_ref()).as_ptr());
                    ptr
                }
                v => duckdb_create_logical_type(match v {
                    Value::Null => DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL,
                    Value::Boolean(..) => DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
                    Value::Int8(..) => DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
                    Value::Int16(..) => DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
                    Value::Int32(..) => DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
                    Value::Int64(..) => DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
                    Value::Int128(..) => DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
                    Value::UInt8(..) => DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
                    Value::UInt16(..) => DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
                    Value::UInt32(..) => DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
                    Value::UInt64(..) => DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
                    Value::UInt128(..) => DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT,
                    Value::Float32(..) => DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
                    Value::Float64(..) => DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
                    Value::Char(..) | Value::Varchar(..) | Value::IpAddr(..) => {
                        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
                    }
                    Value::Blob(..) => DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
                    Value::Date(..) => DUCKDB_TYPE_DUCKDB_TYPE_DATE,
                    Value::Time(..) => DUCKDB_TYPE_DUCKDB_TYPE_TIME,
                    Value::Timestamp(..) => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
                    Value::TimestampWithTimezone(..) => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
                    Value::Interval(..) => DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
                    Value::Uuid(..) => DUCKDB_TYPE_DUCKDB_TYPE_UUID,
                    _ => {
                        log::error!("Value `{v:?}` is unsupported by DuckDB");
                        DUCKDB_TYPE_DUCKDB_TYPE_INVALID
                    }
                }),
            })
        }
    }
}

impl TryFrom<&Value<'_>> for CBox<duckdb_value> {
    type Error = Error;

    fn try_from(value: &Value<'_>) -> Result<Self> {
        let result = CBox(unsafe {
            match value {
                Value::Boolean(Some(v)) => duckdb_create_bool(*v),
                Value::Int8(Some(v)) => duckdb_create_int8(*v),
                Value::Int16(Some(v)) => duckdb_create_int16(*v),
                Value::Int32(Some(v)) => duckdb_create_int32(*v),
                Value::Int64(Some(v)) => duckdb_create_int64(*v),
                Value::Int128(Some(v)) => duckdb_create_hugeint(duckdb_hugeint {
                    lower: **v as _,
                    upper: (**v >> 64) as _,
                }),
                Value::UInt8(Some(v)) => duckdb_create_uint8(*v),
                Value::UInt16(Some(v)) => duckdb_create_uint16(*v),
                Value::UInt32(Some(v)) => duckdb_create_uint32(*v),
                Value::UInt64(Some(v)) => duckdb_create_uint64(*v),
                Value::UInt128(Some(v)) => duckdb_create_uhugeint(duckdb_uhugeint {
                    lower: **v as _,
                    upper: (**v >> 64) as _,
                }),
                Value::Float32(Some(v)) => duckdb_create_float(*v),
                Value::Float64(Some(v)) => duckdb_create_double(*v),
                Value::Decimal(Some(v), w, s) => {
                    duckdb_create_decimal(to_duckdb_decimal(v, *w, *s))
                }
                Value::Char(Some(v)) => {
                    let mut buf = [0u8; 4];
                    let s = v.encode_utf8(&mut buf);
                    duckdb_create_varchar_length(s.as_ptr() as _, s.len() as _)
                }
                Value::Varchar(Some(v)) => {
                    duckdb_create_varchar_length(v.as_ptr() as _, v.len() as _)
                }
                Value::Blob(Some(v)) => duckdb_create_blob(v.as_ptr(), v.len() as _),
                Value::Date(Some(v)) => duckdb_create_date(duckdb_date {
                    days: v.to_julian_day() - Interval::EPOCH_JULIAN_DAY,
                }),
                Value::Time(Some(v)) => duckdb_create_time(to_duckdb_time(v)),
                Value::Timestamp(Some(v)) => duckdb_create_timestamp(to_duckdb_timestamp(v)),
                Value::TimestampWithTimezone(Some(v)) => {
                    duckdb_create_timestamp_tz(duckdb_timestamp {
                        micros: (v.unix_timestamp_nanos() / 1_000) as _,
                    })
                }
                Value::Interval(Some(v)) => duckdb_create_interval(duckdb_interval {
                    months: v.months,
                    days: v.days,
                    micros: v.nanos / 1_000,
                }),
                Value::Uuid(Some(v)) => {
                    let n = v.as_u128();
                    duckdb_create_uuid(duckdb_uhugeint {
                        lower: n as _,
                        upper: (n >> 64) as _,
                    })
                }
                Value::IpAddr(Some(v)) => {
                    let address = v.to_string();
                    duckdb_create_varchar_length(address.as_ptr() as _, address.len() as _)
                }
                Value::Array(Some(v), t) | Value::List(Some(v), t) => {
                    let mut values = v.iter().map(Self::try_from).collect::<Result<Box<[_]>>>()?;
                    let create = if matches!(value, Value::Array(..)) {
                        duckdb_create_array_value
                    } else {
                        duckdb_create_list_value
                    };
                    create(
                        *CBox::<duckdb_logical_type>::from(&t.element),
                        values.as_mut_ptr() as _,
                        v.len() as _,
                    )
                }
                Value::Map(Some(v), ..) => {
                    let mut keys = v.keys().map(Self::try_from).collect::<Result<Box<[_]>>>()?;
                    let mut values = v
                        .values()
                        .map(Self::try_from)
                        .collect::<Result<Box<[_]>>>()?;
                    duckdb_create_map_value(
                        *CBox::<duckdb_logical_type>::from(value),
                        keys.as_mut_ptr() as _,
                        values.as_mut_ptr() as _,
                        v.len() as _,
                    )
                }
                Value::Struct(Some(v), ..) => {
                    let mut values = v
                        .iter()
                        .map(|(_, val)| Self::try_from(val))
                        .collect::<Result<Box<[_]>>>()?;
                    duckdb_create_struct_value(
                        *CBox::<duckdb_logical_type>::from(value),
                        values.as_mut_ptr() as _,
                    )
                }
                v if v.is_null() => duckdb_create_null_value(),
                _ => ptr::null_mut(),
            }
        });
        if result.is_null() {
            hint::cold_path();
            let error = anyhow!("Could not create a DuckDB value for {value:?}");
            log::error!("{error:#}");
            return Err(error);
        }
        Ok(result)
    }
}

#[inline(always)]
pub(crate) fn to_duckdb_decimal(v: &Decimal, width: u8, scale: u8) -> duckdb_decimal {
    let scale = min(scale, 38);
    let d_scale = v.scale() as u8;
    let mantissa = if d_scale == scale {
        v.mantissa()
    } else if d_scale < scale {
        v.mantissa()
            .saturating_mul(10_i128.pow((scale - d_scale) as _))
    } else {
        let div = 10_i128.pow((d_scale - scale) as _);
        (v.mantissa() + (div / 2) * v.mantissa().signum()) / div
    };
    let width = if width == 0 || width < scale {
        max(
            scale as u32,
            mantissa.unsigned_abs().checked_ilog10().unwrap_or(0) + 1,
        )
        .clamp(1, 38) as u8
    } else {
        min(width, 38)
    };
    duckdb_decimal {
        width,
        scale,
        value: duckdb_hugeint {
            lower: mantissa as _,
            upper: (mantissa >> 64) as _,
        },
    }
}

#[inline(always)]
pub(crate) const fn to_duckdb_time(v: &Time) -> duckdb_time {
    let (h, m, s, us) = v.as_hms_micro();
    duckdb_time {
        micros: (h as i64 * 3_600 + m as i64 * 60 + s as i64) * 1_000_000 + us as i64,
    }
}

#[inline(always)]
pub(crate) const fn to_duckdb_timestamp(v: &PrimitiveDateTime) -> duckdb_timestamp {
    duckdb_timestamp {
        micros: (v.date().to_julian_day() - Interval::EPOCH_JULIAN_DAY) as i64
            * Interval::MICROS_IN_DAY as i64
            + to_duckdb_time(&v.time()).micros,
    }
}

macro_rules! ensure_rc {
    ($value:expr, $error:expr $(, $context:expr)? $(,)?) => {{
        let value = $value;
        if value != ::libduckdb_sys::duckdb_state_DuckDBSuccess {
            ::std::hint::cold_path();
            return Err($error)
                $(.context($context))?
                .inspect_err(|e| ::log::error!("{e:#}"));
        }
        value
    }};
}
pub(crate) use ensure_rc;
