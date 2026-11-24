use chdb_rust::arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
        Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
        MapArray, RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, TimeUnit},
};
use std::{
    borrow::Cow,
    cmp::max,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    net::{IpAddr, Ipv6Addr},
    str,
    sync::Arc,
};
use tank_core::{
    CollectionType, MapType, Row, RowLabels, RowOwned, Value, ValueOwned, decimal_from_scaled,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

/// A zero-copy row view borrowing an Arrow `RecordBatch` returned by chDB.
pub struct ChDBRow {
    pub(crate) batch: Arc<(RecordBatch, RowLabels)>,
    pub(crate) row: usize,
}

impl ChDBRow {
    /// Unix epoch `1970-01-01` as a Julian day number.
    const EPOCH_JULIAN_DAY: i32 = 2_440_588;

    #[inline(always)]
    fn extract<'a>(
        array: &'a dyn Array,
        batch: Option<(&'a RecordBatch, usize)>,
        field: Option<&Field>,
        row: usize,
    ) -> Option<Cow<'a, Value<'a>>> {
        let value = match array.data_type() {
            DataType::Boolean => {
                let a = array.as_any().downcast_ref::<BooleanArray>()?;
                if a.nulls().is_some_and(|n| n.is_null(row)) {
                    Value::Null
                } else {
                    Value::Boolean(Some(a.values().value(row)))
                }
            }
            DataType::Int32 => {
                let a = array.as_any().downcast_ref::<Int32Array>()?;
                if a.nulls().is_some_and(|n| n.is_null(row)) {
                    Value::Null
                } else {
                    Value::Int32(Some(*a.values().get(row)?))
                }
            }
            DataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>()?;
                if a.nulls().is_some_and(|n| n.is_null(row)) {
                    Value::Null
                } else {
                    Value::Int64(Some(*a.values().get(row)?))
                }
            }
            DataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>()?;
                if a.nulls().is_some_and(|n| n.is_null(row)) {
                    Value::Null
                } else {
                    Value::Float64(Some(*a.values().get(row)?))
                }
            }
            DataType::Utf8 => {
                let a = array.as_any().downcast_ref::<StringArray>()?;
                if a.nulls().is_some_and(|n| n.is_null(row)) {
                    Value::Null
                } else {
                    Value::Varchar(Some(Cow::Borrowed(a.value(row))))
                }
            }
            _ => {
                // Return `Option<Value<'_>>` (different from `extract`) so rustc keeps hot arms in registers.
                let res = complex(array, batch, field, row);
                #[inline(never)]
                fn complex<'a>(
                    array: &'a dyn Array,
                    batch: Option<(&'a RecordBatch, usize)>,
                    field: Option<&Field>,
                    row: usize,
                ) -> Option<Value<'a>> {
                    if array.is_null(row) {
                        return Some(Value::Null);
                    }
                    fn prototype(data_type: &DataType) -> ValueOwned {
                        match data_type {
                            DataType::Boolean => Value::Boolean(None),
                            DataType::Int8 => Value::Int8(None),
                            DataType::Int16 => Value::Int16(None),
                            DataType::Int32 => Value::Int32(None),
                            DataType::Int64 => Value::Int64(None),
                            DataType::UInt8 => Value::UInt8(None),
                            DataType::UInt16 => Value::UInt16(None),
                            DataType::UInt32 => Value::UInt32(None),
                            DataType::UInt64 => Value::UInt64(None),
                            DataType::Float32 => Value::Float32(None),
                            DataType::Float64 => Value::Float64(None),
                            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                                Value::Varchar(None)
                            }
                            DataType::Binary
                            | DataType::LargeBinary
                            | DataType::FixedSizeBinary(_) => Value::Blob(None),
                            DataType::Date32 | DataType::Date64 => Value::Date(None),
                            DataType::Timestamp(_, Some(tz)) if tz.as_ref() != "UTC" => {
                                Value::TimestampWithTimezone(None)
                            }
                            DataType::Timestamp(_, _) => Value::Timestamp(None),
                            DataType::Decimal32(p, s)
                            | DataType::Decimal64(p, s)
                            | DataType::Decimal128(p, s) => {
                                Value::Decimal(None, *p, max(*s, 0) as u8)
                            }
                            DataType::Null => Value::Null,
                            DataType::List(f) | DataType::LargeList(f) => Value::List(
                                None,
                                Box::new(CollectionType {
                                    element: prototype(f.data_type()),
                                    len: 0,
                                }),
                            ),
                            DataType::Map(entry, _) => match entry.data_type() {
                                DataType::Struct(fields) if fields.len() == 2 => Value::Map(
                                    None,
                                    Box::new(MapType {
                                        key: prototype(fields[0].data_type()),
                                        value: prototype(fields[1].data_type()),
                                    }),
                                ),
                                _ => Value::Unknown(None),
                            },
                            _ => Value::Unknown(None),
                        }
                    }

                    let field = field.or_else(|| {
                        let (b, index) = batch?;
                        b.schema_ref().fields().get(index).map(Arc::as_ref)
                    });
                    Some(match array.data_type() {
                        DataType::Null => Value::Null,
                        DataType::Int8 => Value::Int8(Some(
                            array.as_any().downcast_ref::<Int8Array>()?.value(row),
                        )),
                        DataType::Int16 => Value::Int16(Some(
                            array.as_any().downcast_ref::<Int16Array>()?.value(row),
                        )),
                        DataType::UInt8 => Value::UInt8(Some(
                            array.as_any().downcast_ref::<UInt8Array>()?.value(row),
                        )),
                        DataType::UInt16 => Value::UInt16(Some(
                            array.as_any().downcast_ref::<UInt16Array>()?.value(row),
                        )),
                        DataType::UInt32 => Value::UInt32(Some(
                            array.as_any().downcast_ref::<UInt32Array>()?.value(row),
                        )),
                        DataType::UInt64 => Value::UInt64(Some(
                            array.as_any().downcast_ref::<UInt64Array>()?.value(row),
                        )),
                        DataType::Float32 => Value::Float32(Some(
                            array.as_any().downcast_ref::<Float32Array>()?.value(row),
                        )),
                        DataType::LargeUtf8 => Value::Varchar(Some(Cow::Borrowed(
                            array
                                .as_any()
                                .downcast_ref::<LargeStringArray>()?
                                .value(row),
                        ))),
                        DataType::Utf8View => Value::Varchar(Some(Cow::Borrowed(
                            array.as_any().downcast_ref::<StringViewArray>()?.value(row),
                        ))),
                        DataType::Binary => {
                            let bytes = array.as_any().downcast_ref::<BinaryArray>()?.value(row);
                            match str::from_utf8(bytes) {
                                Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                                Err(_) => Value::Blob(Some(Cow::Borrowed(bytes))),
                            }
                        }
                        DataType::LargeBinary => {
                            let bytes = array
                                .as_any()
                                .downcast_ref::<LargeBinaryArray>()?
                                .value(row);
                            match str::from_utf8(bytes) {
                                Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                                Err(_) => Value::Blob(Some(Cow::Borrowed(bytes))),
                            }
                        }
                        DataType::FixedSizeBinary(len) => {
                            let bytes = array
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()?
                                .value(row);
                            if *len == 16
                                && let Ok(octets) = <[u8; 16]>::try_from(bytes)
                            {
                                let is_uuid = field
                                    .and_then(|f| f.metadata().get("ARROW:extension:name"))
                                    .is_some_and(|name| name == "arrow.uuid");
                                if is_uuid {
                                    return Some(Value::Uuid(Some(Uuid::from_bytes(octets))));
                                }
                                let v6 = Ipv6Addr::from(octets);
                                return Some(Value::IpAddr(Some(
                                    v6.to_ipv4_mapped()
                                        .map(IpAddr::V4)
                                        .unwrap_or(IpAddr::V6(v6)),
                                )));
                            }
                            let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
                            match str::from_utf8(&bytes[..end]) {
                                Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                                Err(_) => Value::Blob(Some(Cow::Borrowed(bytes))),
                            }
                        }
                        DataType::Date32 => {
                            let days = array.as_any().downcast_ref::<Date32Array>()?.value(row);
                            Value::Date(
                                ChDBRow::EPOCH_JULIAN_DAY
                                    .checked_add(days)
                                    .and_then(|jd| Date::from_julian_day(jd).ok()),
                            )
                        }
                        DataType::Date64 => {
                            let ms = array.as_any().downcast_ref::<Date64Array>()?.value(row);
                            let days = ms.div_euclid(86_400_000) as i32;
                            Value::Date(
                                ChDBRow::EPOCH_JULIAN_DAY
                                    .checked_add(days)
                                    .and_then(|jd| Date::from_julian_day(jd).ok()),
                            )
                        }
                        DataType::Timestamp(TimeUnit::Nanosecond, tz)
                            if tz.as_deref().is_none_or(|t| t == "UTC") =>
                        {
                            const DAY_SECS: u64 = 86_400;
                            const DAY_NANOS: i64 = 86_400_000_000_000;
                            let nanos = array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()?
                                .value(row);
                            let (days, secs, ns) = if nanos >= 0 {
                                let u = nanos as u64;
                                let total_secs = u / 1_000_000_000;
                                let ns = (u - total_secs * 1_000_000_000) as u32;
                                let days = (total_secs / DAY_SECS) as i32;
                                let secs = (total_secs - days as u64 * DAY_SECS) as u32;
                                (days, secs, ns)
                            } else {
                                let days = nanos.div_euclid(DAY_NANOS) as i32;
                                let rem = (nanos - days as i64 * DAY_NANOS) as u64;
                                let secs = (rem / 1_000_000_000) as u32;
                                let ns = (rem - secs as u64 * 1_000_000_000) as u32;
                                (days, secs, ns)
                            };
                            let mins = secs / 60;
                            let second = (secs - mins * 60) as u8;
                            let hour = (mins / 60) as u8;
                            let minute = (mins - hour as u32 * 60) as u8;
                            match (
                                ChDBRow::EPOCH_JULIAN_DAY
                                    .checked_add(days)
                                    .and_then(|jd| Date::from_julian_day(jd).ok()),
                                Time::from_hms_nano(hour, minute, second, ns).ok(),
                            ) {
                                (Some(date), Some(time)) => {
                                    Value::Timestamp(Some(PrimitiveDateTime::new(date, time)))
                                }
                                _ => Value::Null,
                            }
                        }
                        DataType::Timestamp(unit, tz) => {
                            let nanos: i128 = match unit {
                                TimeUnit::Second => {
                                    array
                                        .as_any()
                                        .downcast_ref::<TimestampSecondArray>()?
                                        .value(row) as i128
                                        * 1_000_000_000
                                }
                                TimeUnit::Millisecond => {
                                    array
                                        .as_any()
                                        .downcast_ref::<TimestampMillisecondArray>()?
                                        .value(row) as i128
                                        * 1_000_000
                                }
                                TimeUnit::Microsecond => {
                                    array
                                        .as_any()
                                        .downcast_ref::<TimestampMicrosecondArray>()?
                                        .value(row) as i128
                                        * 1_000
                                }
                                TimeUnit::Nanosecond => array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()?
                                    .value(row)
                                    as i128,
                            };
                            let Ok(odt) = OffsetDateTime::from_unix_timestamp_nanos(nanos) else {
                                return Some(Value::Null);
                            };
                            if tz.as_deref().is_some_and(|t| t != "UTC") {
                                Value::TimestampWithTimezone(Some(odt))
                            } else {
                                Value::Timestamp(Some(PrimitiveDateTime::new(
                                    odt.date(),
                                    odt.time(),
                                )))
                            }
                        }
                        DataType::Decimal32(p, s) => {
                            let raw = array.as_any().downcast_ref::<Decimal32Array>()?.value(row);
                            let scale = max(*s, 0) as u8;
                            Value::Decimal(
                                decimal_from_scaled(raw as i128, scale as u32).ok(),
                                *p,
                                scale,
                            )
                        }
                        DataType::Decimal64(p, s) => {
                            let raw = array.as_any().downcast_ref::<Decimal64Array>()?.value(row);
                            let scale = max(*s, 0) as u8;
                            Value::Decimal(
                                decimal_from_scaled(raw as i128, scale as u32).ok(),
                                *p,
                                scale,
                            )
                        }
                        DataType::Decimal128(p, s) => {
                            let raw = array.as_any().downcast_ref::<Decimal128Array>()?.value(row);
                            let scale = max(*s, 0) as u8;
                            Value::Decimal(decimal_from_scaled(raw, scale as u32).ok(), *p, scale)
                        }
                        DataType::List(field) => {
                            let list = array.as_any().downcast_ref::<ListArray>()?;
                            let offsets = list.value_offsets();
                            let start = offsets[row] as usize;
                            let end = offsets[row + 1] as usize;
                            let values_arr = list.values().as_ref();
                            let mut out = Vec::with_capacity(end.saturating_sub(start));
                            for i in start..end {
                                out.push(
                                    ChDBRow::extract(values_arr, None, Some(field.as_ref()), i)?
                                        .into_owned(),
                                );
                            }
                            Value::List(
                                Some(out.into_boxed_slice()),
                                Box::new(CollectionType {
                                    element: prototype(field.data_type()),
                                    len: 0,
                                }),
                            )
                        }
                        DataType::LargeList(field) => {
                            let list = array.as_any().downcast_ref::<LargeListArray>()?;
                            let offsets = list.value_offsets();
                            let start = offsets[row] as usize;
                            let end = offsets[row + 1] as usize;
                            let values_arr = list.values().as_ref();
                            let mut out = Vec::with_capacity(end.saturating_sub(start));
                            for i in start..end {
                                out.push(
                                    ChDBRow::extract(values_arr, None, Some(field.as_ref()), i)?
                                        .into_owned(),
                                );
                            }
                            Value::List(
                                Some(out.into_boxed_slice()),
                                Box::new(CollectionType {
                                    element: prototype(field.data_type()),
                                    len: 0,
                                }),
                            )
                        }
                        DataType::Map(entry, _) => {
                            let map = array.as_any().downcast_ref::<MapArray>()?;
                            let offsets = map.value_offsets();
                            let start = offsets[row] as usize;
                            let end = offsets[row + 1] as usize;
                            let keys = map.keys().as_ref();
                            let vals = map.values().as_ref();
                            let (key_field, val_field) = match entry.data_type() {
                                DataType::Struct(fields) if fields.len() == 2 => {
                                    (Some(fields[0].as_ref()), Some(fields[1].as_ref()))
                                }
                                _ => (None, None),
                            };
                            let mut out = HashMap::with_capacity(end.saturating_sub(start));
                            for i in start..end {
                                out.insert(
                                    ChDBRow::extract(keys, None, key_field, i)?.into_owned(),
                                    ChDBRow::extract(vals, None, val_field, i)?.into_owned(),
                                );
                            }
                            let (key, value) = match (key_field, val_field) {
                                (Some(kf), Some(vf)) => {
                                    (prototype(kf.data_type()), prototype(vf.data_type()))
                                }
                                _ => (Value::Unknown(None), Value::Unknown(None)),
                            };
                            Value::Map(Some(Box::new(out)), Box::new(MapType { key, value }))
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

impl Row for ChDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.1
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.0.num_columns()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        let array = self.batch.0.columns().get(index)?.as_ref();
        Self::extract(array, Some((&self.batch.0, index)), None, self.row)
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.1.clone(),
            self.values()
                .map(|value| value.into_owned().into_owned())
                .collect(),
        )
    }
}

impl Debug for ChDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChDBRow")
            .field("labels", &self.batch.1)
            .field("len", &self.len())
            .finish()
    }
}
