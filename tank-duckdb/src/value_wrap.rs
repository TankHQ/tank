use crate::util::CBox;
use anyhow::anyhow;
use libduckdb_sys::*;
use rust_decimal::Decimal;
use std::{
    borrow::Cow,
    cmp::{max, min},
    ffi::{CStr, c_char},
    hint, slice,
};
use tank_core::{
    CollectionType, Error, ErrorContext, Interval, MapType, Result, StructType, TableRef, Value,
    as_c_string, decimal_from_scaled, log_error,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, macros::date};
use uuid::Uuid;

pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, Value<'a>>);

impl ValueWrap<'_> {
    const EPOCH_JULIAN_DAY: i32 = date!(1970 - 01 - 01).to_julian_day();

    #[inline(always)]
    pub(crate) fn from_i128(v: i128) -> duckdb_hugeint {
        duckdb_hugeint {
            lower: v as u64,
            upper: (v >> 64) as i64,
        }
    }

    #[inline(always)]
    pub(crate) fn to_i128(v: &duckdb_hugeint) -> i128 {
        (v.upper as i128) << 64 | v.lower as i128
    }

    #[inline(always)]
    pub(crate) fn from_u128(v: u128) -> duckdb_uhugeint {
        duckdb_uhugeint {
            lower: v as u64,
            upper: (v >> 64) as u64,
        }
    }

    #[inline(always)]
    pub(crate) fn from_decimal(v: &Decimal, width: u8, scale: u8) -> duckdb_decimal {
        let scale = min(scale, 38);
        let d_scale = v.scale() as u8;
        let mantissa = if d_scale == scale {
            v.mantissa()
        } else if d_scale < scale {
            v.mantissa()
                .saturating_mul(10_i128.pow((scale - d_scale) as u32))
        } else {
            let div = 10_i128.pow((d_scale - scale) as u32);
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
            value: Self::from_i128(mantissa),
        }
    }

    pub(crate) fn to_decimal(num: i128, scale: u8) -> Option<Decimal> {
        decimal_from_scaled(num, scale as u32).ok()
    }

    #[inline(always)]
    pub(crate) fn from_date(v: &Date) -> duckdb_date {
        duckdb_date {
            days: v.to_julian_day() - Self::EPOCH_JULIAN_DAY,
        }
    }

    pub(crate) fn to_date(days: i32) -> Option<Date> {
        Date::from_julian_day(days.saturating_add(Self::EPOCH_JULIAN_DAY)).ok()
    }

    #[inline(always)]
    pub(crate) fn from_time(v: &Time) -> duckdb_time {
        let (h, m, s, us) = v.as_hms_micro();
        duckdb_time {
            micros: (h as i64 * 3_600 + m as i64 * 60 + s as i64) * 1_000_000 + us as i64,
        }
    }

    pub(crate) fn to_time_ns(nanos: i64) -> Option<Time> {
        if (nanos as u64) >= 86_400_000_000_000 {
            return None;
        }
        let (secs, ns) = (
            (nanos as u64 / 1_000_000_000) as u32,
            (nanos as u64 % 1_000_000_000) as u32,
        );
        Time::from_hms_nano(
            (secs / 3600) as u8,
            ((secs / 60) % 60) as u8,
            (secs % 60) as u8,
            ns,
        )
        .ok()
    }

    #[inline(always)]
    pub(crate) fn from_timestamp(v: &PrimitiveDateTime) -> duckdb_timestamp {
        duckdb_timestamp {
            micros: Self::from_date(&v.date()).days as i64 * 86_400_000_000
                + Self::from_time(&v.time()).micros,
        }
    }

    pub(crate) fn to_timestamp(micros: i64) -> Option<PrimitiveDateTime> {
        let days = i32::try_from(micros.div_euclid(86_400_000_000)).ok()?;
        let rem = micros.rem_euclid(86_400_000_000) as u64;
        let (secs, us) = ((rem / 1_000_000) as u32, (rem % 1_000_000) as u32);
        Some(PrimitiveDateTime::new(
            Self::to_date(days)?,
            Time::from_hms_micro(
                (secs / 3600) as u8,
                ((secs / 60) % 60) as u8,
                (secs % 60) as u8,
                us,
            )
            .ok()?,
        ))
    }

    #[inline(always)]
    pub(crate) fn from_timestamptz(v: &OffsetDateTime) -> duckdb_timestamp {
        duckdb_timestamp {
            micros: (v.unix_timestamp_nanos() / 1_000) as i64,
        }
    }

    #[inline(always)]
    pub(crate) fn from_interval(v: &Interval) -> duckdb_interval {
        duckdb_interval {
            months: v.months,
            days: v.days,
            micros: v.nanos / 1_000,
        }
    }

    /// DuckDB stores a UUID as a `HUGEINT` with the sign bit flipped, so they sort as unsigned.
    #[inline(always)]
    pub(crate) fn from_uuid(v: &Uuid) -> duckdb_hugeint {
        let (upper, lower) = v.as_u64_pair();
        duckdb_hugeint {
            lower,
            upper: (upper ^ (1 << 63)) as i64,
        }
    }

    #[inline(always)]
    pub(crate) fn to_uuid(v: &duckdb_hugeint) -> Uuid {
        Uuid::from_u64_pair(v.upper as u64 ^ (1 << 63), v.lower)
    }

    #[inline(always)]
    pub(crate) fn bytes_slice(b: &duckdb_string_t) -> &[u8] {
        unsafe {
            let len = b.value.inlined.length as usize;
            let ptr = [
                b.value.pointer.ptr as *const u8,
                b.value.inlined.inlined.as_ptr() as *const u8,
            ][(len <= b.value.inlined.inlined.len()) as usize];
            slice::from_raw_parts(ptr, len)
        }
    }

    /// Typed NULL of a DuckDB type. `logical_type` is only called for the types that need its
    /// details (decimals and nested types), sparing the others a C allocation.
    #[inline(always)]
    pub(crate) unsafe fn empty_value(
        id: DUCKDB_TYPE,
        logical_type: impl FnOnce() -> CBox<duckdb_logical_type>,
    ) -> Option<Value<'static>> {
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
            // A different return type keeps the cold call from sharing the caller's return slot.
            id => return Self::detailed_empty_value(id, logical_type()).ok(),
        })
    }

    /// The part of [`Self::empty_value`] reading the logical type.
    #[cold]
    #[inline(never)]
    fn detailed_empty_value(
        id: DUCKDB_TYPE,
        owned: CBox<duckdb_logical_type>,
    ) -> Result<Value<'static>> {
        let logical_type = *owned;
        let child = |child| unsafe {
            let child = CBox(child);
            let id = duckdb_get_type_id(*child);
            Self::empty_value(id, || child)
                .with_context(|| format!("Unsupported DuckDB logical type: {id}"))
        };
        Ok(unsafe {
            match id {
                DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Value::Decimal(
                    None,
                    duckdb_decimal_width(logical_type),
                    duckdb_decimal_scale(logical_type),
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_LIST => Value::List(
                    None,
                    Box::new(CollectionType {
                        element: child(duckdb_list_type_child_type(logical_type))?,
                        len: 0,
                    }),
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => Value::Array(
                    None,
                    Box::new(CollectionType {
                        element: child(duckdb_array_type_child_type(logical_type))?,
                        len: duckdb_array_type_array_size(logical_type) as u32,
                    }),
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => Value::Struct(
                    None,
                    Box::new(StructType {
                        fields: (0..duckdb_struct_type_child_count(logical_type))
                            .map(|i| {
                                Ok((
                                    Self::take_string(duckdb_struct_type_child_name(
                                        logical_type,
                                        i,
                                    )),
                                    child(duckdb_struct_type_child_type(logical_type, i))?,
                                ))
                            })
                            .collect::<Result<_>>()?,
                        name: TableRef::new(
                            Self::take_string(duckdb_logical_type_get_alias(logical_type)).into(),
                        ),
                    }),
                ),
                DUCKDB_TYPE_DUCKDB_TYPE_MAP => Value::Map(
                    None,
                    Box::new(MapType {
                        key: child(duckdb_map_type_key_type(logical_type))?,
                        value: child(duckdb_map_type_value_type(logical_type))?,
                    }),
                ),
                id => return Err(anyhow!("Unsupported DuckDB logical type: {id}")),
            }
        })
    }

    fn take_string(ptr: *mut c_char) -> String {
        let ptr = CBox(ptr);
        if ptr.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(*ptr).to_string_lossy().into_owned() }
        }
    }
}

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

impl From<ValueWrap<'_>> for CBox<duckdb_logical_type> {
    fn from(value: ValueWrap<'_>) -> Self {
        unsafe {
            CBox(match value.0.as_ref() {
                Value::Decimal(_, w, s) => duckdb_create_decimal_type(*w, *s),
                Value::Array(_, t) => {
                    duckdb_create_array_type(*Self::from(ValueWrap::from(&t.element)), t.len as _)
                }
                Value::List(_, t) => {
                    duckdb_create_list_type(*Self::from(ValueWrap::from(&t.element)))
                }
                Value::Map(_, t) => duckdb_create_map_type(
                    *Self::from(ValueWrap::from(&t.key)),
                    *Self::from(ValueWrap::from(&t.value)),
                ),
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
                        .map(|(_, v)| Self::from(ValueWrap::from(v)))
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
                    Value::Char(..) | Value::Varchar(..) => DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
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

impl TryFrom<ValueWrap<'_>> for CBox<duckdb_value> {
    type Error = Error;

    fn try_from(wrap: ValueWrap<'_>) -> Result<Self> {
        let value = wrap.0.as_ref();
        let result = CBox(unsafe {
            match value {
                Value::Boolean(Some(v)) => duckdb_create_bool(*v),
                Value::Int8(Some(v)) => duckdb_create_int8(*v),
                Value::Int16(Some(v)) => duckdb_create_int16(*v),
                Value::Int32(Some(v)) => duckdb_create_int32(*v),
                Value::Int64(Some(v)) => duckdb_create_int64(*v),
                Value::Int128(Some(v)) => duckdb_create_hugeint(ValueWrap::from_i128(**v)),
                Value::UInt8(Some(v)) => duckdb_create_uint8(*v),
                Value::UInt16(Some(v)) => duckdb_create_uint16(*v),
                Value::UInt32(Some(v)) => duckdb_create_uint32(*v),
                Value::UInt64(Some(v)) => duckdb_create_uint64(*v),
                Value::UInt128(Some(v)) => duckdb_create_uhugeint(ValueWrap::from_u128(**v)),
                Value::Float32(Some(v)) => duckdb_create_float(*v),
                Value::Float64(Some(v)) => duckdb_create_double(*v),
                Value::Decimal(Some(v), w, s) => {
                    duckdb_create_decimal(ValueWrap::from_decimal(v, *w, *s))
                }
                Value::Char(Some(v)) => {
                    let mut buffer = [0u8; 4];
                    let s = v.encode_utf8(&mut buffer);
                    duckdb_create_varchar_length(s.as_ptr() as _, s.len() as _)
                }
                Value::Varchar(Some(v)) => {
                    duckdb_create_varchar_length(v.as_ptr() as _, v.len() as _)
                }
                Value::Blob(Some(v)) => duckdb_create_blob(v.as_ptr(), v.len() as _),
                Value::Date(Some(v)) => duckdb_create_date(ValueWrap::from_date(v)),
                Value::Time(Some(v)) => duckdb_create_time(ValueWrap::from_time(v)),
                Value::Timestamp(Some(v)) => duckdb_create_timestamp(ValueWrap::from_timestamp(v)),
                Value::TimestampWithTimezone(Some(v)) => {
                    duckdb_create_timestamp_tz(ValueWrap::from_timestamptz(v))
                }
                Value::Interval(Some(v)) => duckdb_create_interval(ValueWrap::from_interval(v)),
                Value::Uuid(Some(v)) => duckdb_create_uuid(ValueWrap::from_u128(v.as_u128())),
                Value::Array(Some(v), t) | Value::List(Some(v), t) => {
                    let mut values = v
                        .iter()
                        .map(|v| Self::try_from(ValueWrap::from(v)))
                        .collect::<Result<Box<[_]>>>()?;
                    let create = if matches!(value, Value::Array(..)) {
                        duckdb_create_array_value
                    } else {
                        duckdb_create_list_value
                    };
                    create(
                        *CBox::<duckdb_logical_type>::from(ValueWrap::from(&t.element)),
                        values.as_mut_ptr() as _,
                        v.len() as _,
                    )
                }
                Value::Map(Some(v), ..) => {
                    let mut keys = v
                        .keys()
                        .map(|k| Self::try_from(ValueWrap::from(k)))
                        .collect::<Result<Box<[_]>>>()?;
                    let mut values = v
                        .values()
                        .map(|val| Self::try_from(ValueWrap::from(val)))
                        .collect::<Result<Box<[_]>>>()?;
                    duckdb_create_map_value(
                        *CBox::<duckdb_logical_type>::from(ValueWrap::from(value)),
                        keys.as_mut_ptr() as _,
                        values.as_mut_ptr() as _,
                        v.len() as _,
                    )
                }
                Value::Struct(Some(v), ..) => {
                    let mut values = v
                        .iter()
                        .map(|(_, val)| Self::try_from(ValueWrap::from(val)))
                        .collect::<Result<Box<[_]>>>()?;
                    duckdb_create_struct_value(
                        *CBox::<duckdb_logical_type>::from(ValueWrap::from(value)),
                        values.as_mut_ptr() as _,
                    )
                }
                v if v.is_null() => duckdb_create_null_value(),
                _ => Default::default(),
            }
        });
        if result.is_null() {
            hint::cold_path();
            return Err(anyhow!("Could not create a DuckDB value for {value:?}"))
                .inspect_err(log_error);
        }
        Ok(result)
    }
}
