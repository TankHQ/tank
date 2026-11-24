use crate::{batch::ChDBBatch, cursor::Cursor};
use anyhow::{anyhow, ensure};
use std::{
    borrow::Cow,
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ptr, slice, str,
};
use tank_core::{CollectionType, MapType, Result, Value, ValueOwned, decimal_from_scaled};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

#[derive(Debug)]
enum ColumnKind {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float32,
    Float64,
    String(u32),
    FixedString(u32),
    Date,
    Date32,
    DateTime(bool),
    DateTime64(u8, bool),
    Decimal32(u8, u8),
    Decimal64(u8, u8),
    Decimal128(u8, u8),
    Uuid,
    IPv4,
    IPv6,
    Nothing,
    // Boxed to keep `ColumnKind` compact (16 bytes) and avoid inflating scalar columns.
    LowCardinality(Box<(Column, u8, bool)>),
    Array(Box<Column>),
    Map(Box<(Column, Column)>),
}

#[derive(Debug)]
pub(crate) struct Column {
    nulls: *const u8,
    data: *const u8,
    kind: ColumnKind,
}

impl Column {
    /// Unix epoch `1970-01-01` as a Julian day number.
    const EPOCH_JULIAN_DAY: i32 = 2_440_588;

    pub(crate) fn parse(
        ty: &str,
        rows: usize,
        cursor: &mut Cursor<'_>,
        spans: &mut Vec<(u32, u32)>,
        nulls: *const u8,
    ) -> Result<Self> {
        let ty = ty.trim();
        if let Some(inner) = Self::strip_wrapper(ty, "Nullable") {
            let nulls = if rows > 0 {
                cursor.read_bytes(rows)?.as_ptr()
            } else {
                ptr::null()
            };
            return Self::parse(inner, rows, cursor, spans, nulls);
        }
        if let Some(inner) = Self::strip_wrapper(ty, "LowCardinality") {
            if rows == 0 {
                return Ok(Self {
                    nulls,
                    data: ptr::null(),
                    kind: ColumnKind::Nothing,
                });
            }
            let version = cursor.read_u64_le()?;
            ensure!(version == 1, "Unsupported LowCardinality version {version}");
            let flags = cursor.read_u64_le()?;
            let index_shift = (flags & 0xff) as u8;
            ensure!(
                index_shift <= 3,
                "Unsupported LowCardinality index width {index_shift}"
            );
            let inner_nullable = Self::strip_wrapper(inner, "Nullable");
            let dict_ty = inner_nullable.unwrap_or(inner);
            let dict_size = if (flags & (1 << 9)) != 0 {
                cursor.read_u64_le()? as usize
            } else {
                0
            };
            let dict = Self::parse(dict_ty, dict_size, cursor, spans, ptr::null())?;
            let indices_size = cursor.read_u64_le()? as usize;
            ensure!(
                indices_size == rows,
                "LowCardinality index count {indices_size} differs from block rows {rows}"
            );
            let indices = cursor.read_bytes(rows << index_shift)?.as_ptr();
            return Ok(Self {
                nulls,
                data: indices,
                kind: ColumnKind::LowCardinality(Box::new((
                    dict,
                    index_shift,
                    inner_nullable.is_some(),
                ))),
            });
        }
        if let Some(inner) = Self::strip_wrapper(ty, "Array") {
            let (offsets, total) = Self::read_offsets(rows, cursor)?;
            let element = Self::parse(inner, total, cursor, spans, ptr::null())?;
            return Ok(Self {
                nulls,
                data: offsets.cast(),
                kind: ColumnKind::Array(Box::new(element)),
            });
        }
        if let Some(inner) = Self::strip_wrapper(ty, "Map") {
            let (k_ty, v_ty) = Self::split_top_level_comma(inner)
                .ok_or_else(|| anyhow!("Malformed ClickHouse Map type: {ty}"))?;
            let (offsets, total) = Self::read_offsets(rows, cursor)?;
            let key = Self::parse(k_ty, total, cursor, spans, ptr::null())?;
            let value = Self::parse(v_ty, total, cursor, spans, ptr::null())?;
            return Ok(Self {
                nulls,
                data: offsets.cast(),
                kind: ColumnKind::Map(Box::new((key, value))),
            });
        }
        if ty == "String" {
            ensure!(
                cursor.data_len() <= u32::MAX as usize,
                "ClickHouse Native chunk exceeds 4 GiB"
            );
            let base = u32::try_from(spans.len())
                .map_err(|_| anyhow!("Too many string cells in ClickHouse Native block"))?;
            spans.reserve(rows);
            for _ in 0..rows {
                let len = cursor.read_varuint()?;
                let start = cursor.pos();
                cursor.read_bytes(len)?;
                spans.push((start as u32, len as u32));
            }
            return Ok(Self {
                nulls,
                data: cursor.data_ptr(),
                kind: ColumnKind::String(base),
            });
        }
        let (kind, width) = Self::parse_fixed_kind(ty)?;
        let bytes = rows
            .checked_mul(width)
            .ok_or_else(|| anyhow!("ClickHouse Native column size overflow"))?;
        let data = if bytes > 0 {
            cursor.read_bytes(bytes)?.as_ptr()
        } else {
            ptr::null()
        };
        Ok(Self { nulls, data, kind })
    }

    fn read_offsets(rows: usize, cursor: &mut Cursor<'_>) -> Result<(*const u64, usize)> {
        if rows == 0 {
            return Ok((ptr::null(), 0));
        }
        let bytes = rows
            .checked_mul(8)
            .ok_or_else(|| anyhow!("ClickHouse Native offset size overflow"))?;
        let ptr = cursor.read_bytes(bytes)?.as_ptr() as *const u64;
        let total = unsafe { ptr.add(rows - 1).read_unaligned() };
        let total = usize::try_from(total)
            .map_err(|_| anyhow!("ClickHouse Native collection length overflow"))?;
        Ok((ptr, total))
    }

    unsafe fn offset_range(data: *const u8, row: usize) -> (usize, usize) {
        let offsets = data as *const u64;
        let start = if row == 0 {
            0
        } else {
            unsafe { offsets.add(row - 1).read_unaligned() as usize }
        };
        let end = unsafe { offsets.add(row).read_unaligned() as usize };
        (start, end)
    }

    fn strip_wrapper<'a>(ty: &'a str, name: &str) -> Option<&'a str> {
        ty.strip_prefix(name)?.strip_prefix('(')?.strip_suffix(')')
    }

    fn split_top_level_comma(s: &str) -> Option<(&str, &str)> {
        let mut depth = 0usize;
        let mut in_quote = false;
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' if in_quote => i += 1,
                b'\'' => in_quote = !in_quote,
                b'(' if !in_quote => depth += 1,
                b')' if !in_quote => depth = depth.saturating_sub(1),
                b',' if !in_quote && depth == 0 => return Some((&s[..i], &s[i + 1..])),
                _ => {}
            }
            i += 1;
        }
        None
    }

    fn parse_u8(s: &str, ty: &str) -> Result<u8> {
        s.trim()
            .parse()
            .map_err(|_| anyhow!("Invalid numeric parameter in `{ty}`"))
    }

    fn parse_fixed_kind(ty: &str) -> Result<(ColumnKind, usize)> {
        Ok(match ty {
            "Bool" => (ColumnKind::Bool, 1),
            "Int8" => (ColumnKind::Int8, 1),
            "Int16" => (ColumnKind::Int16, 2),
            "Int32" => (ColumnKind::Int32, 4),
            "Int64" => (ColumnKind::Int64, 8),
            "Int128" => (ColumnKind::Int128, 16),
            "UInt8" => (ColumnKind::UInt8, 1),
            "UInt16" => (ColumnKind::UInt16, 2),
            "UInt32" => (ColumnKind::UInt32, 4),
            "UInt64" => (ColumnKind::UInt64, 8),
            "UInt128" => (ColumnKind::UInt128, 16),
            "Float32" => (ColumnKind::Float32, 4),
            "Float64" => (ColumnKind::Float64, 8),
            "Date" => (ColumnKind::Date, 2),
            "Date32" => (ColumnKind::Date32, 4),
            "DateTime" => (ColumnKind::DateTime(false), 4),
            "UUID" => (ColumnKind::Uuid, 16),
            "IPv4" => (ColumnKind::IPv4, 4),
            "IPv6" => (ColumnKind::IPv6, 16),
            "Nothing" => (ColumnKind::Nothing, 1),
            _ if Self::strip_wrapper(ty, "FixedString").is_some() => {
                let n: u32 = Self::strip_wrapper(ty, "FixedString")
                    .unwrap_or_default()
                    .trim()
                    .parse()
                    .map_err(|_| anyhow!("Invalid FixedString length in `{ty}`"))?;
                (ColumnKind::FixedString(n), n as usize)
            }
            _ if Self::strip_wrapper(ty, "DateTime").is_some() => {
                let tz = Self::strip_wrapper(ty, "DateTime")
                    .unwrap_or_default()
                    .trim()
                    .trim_matches('\'');
                (ColumnKind::DateTime(tz != "UTC"), 4)
            }
            _ if Self::strip_wrapper(ty, "DateTime64").is_some() => {
                let inner = Self::strip_wrapper(ty, "DateTime64").unwrap_or_default();
                let (scale, non_utc) = match Self::split_top_level_comma(inner) {
                    Some((s, tz)) => (Self::parse_u8(s, ty)?, tz.trim().trim_matches('\'') != "UTC"),
                    None => (Self::parse_u8(inner, ty)?, false),
                };
                (ColumnKind::DateTime64(scale, non_utc), 8)
            }
            _ if Self::strip_wrapper(ty, "Decimal").is_some() => {
                let inner = Self::strip_wrapper(ty, "Decimal").unwrap_or_default();
                let (p_str, s_str) = Self::split_top_level_comma(inner)
                    .ok_or_else(|| anyhow!("Invalid Decimal type `{ty}`"))?;
                let (p, s) = (Self::parse_u8(p_str, ty)?, Self::parse_u8(s_str, ty)?);
                match p {
                    1..=9 => (ColumnKind::Decimal32(p, s), 4),
                    10..=18 => (ColumnKind::Decimal64(p, s), 8),
                    19..=38 => (ColumnKind::Decimal128(p, s), 16),
                    _ => return Err(anyhow!("Unsupported Decimal precision in `{ty}`")),
                }
            }
            _ if Self::strip_wrapper(ty, "Decimal32").is_some() => (
                ColumnKind::Decimal32(9, Self::parse_u8(Self::strip_wrapper(ty, "Decimal32").unwrap_or_default(), ty)?),
                4,
            ),
            _ if Self::strip_wrapper(ty, "Decimal64").is_some() => (
                ColumnKind::Decimal64(18, Self::parse_u8(Self::strip_wrapper(ty, "Decimal64").unwrap_or_default(), ty)?),
                8,
            ),
            _ if Self::strip_wrapper(ty, "Decimal128").is_some() => (
                ColumnKind::Decimal128(38, Self::parse_u8(Self::strip_wrapper(ty, "Decimal128").unwrap_or_default(), ty)?),
                16,
            ),
            _ if Self::strip_wrapper(ty, "Enum8").is_some() => (ColumnKind::Int8, 1),
            _ if Self::strip_wrapper(ty, "Enum16").is_some() => (ColumnKind::Int16, 2),
            _ => return Err(anyhow!("Unsupported ClickHouse Native column type `{ty}`")),
        })
    }

    fn prototype(&self) -> ValueOwned {
        match &self.kind {
            ColumnKind::Bool => Value::Boolean(None),
            ColumnKind::Int8 => Value::Int8(None),
            ColumnKind::Int16 => Value::Int16(None),
            ColumnKind::Int32 => Value::Int32(None),
            ColumnKind::Int64 => Value::Int64(None),
            ColumnKind::Int128 => Value::Int128(None),
            ColumnKind::UInt8 => Value::UInt8(None),
            ColumnKind::UInt16 => Value::UInt16(None),
            ColumnKind::UInt32 => Value::UInt32(None),
            ColumnKind::UInt64 => Value::UInt64(None),
            ColumnKind::UInt128 => Value::UInt128(None),
            ColumnKind::Float32 => Value::Float32(None),
            ColumnKind::Float64 => Value::Float64(None),
            ColumnKind::String(_) | ColumnKind::FixedString(_) => Value::Varchar(None),
            ColumnKind::Date | ColumnKind::Date32 => Value::Date(None),
            ColumnKind::DateTime(true) | ColumnKind::DateTime64(_, true) => {
                Value::TimestampWithTimezone(None)
            }
            ColumnKind::DateTime(false) | ColumnKind::DateTime64(_, false) => {
                Value::Timestamp(None)
            }
            ColumnKind::Decimal32(p, s)
            | ColumnKind::Decimal64(p, s)
            | ColumnKind::Decimal128(p, s) => Value::Decimal(None, *p, *s),
            ColumnKind::Uuid => Value::Uuid(None),
            ColumnKind::IPv4 | ColumnKind::IPv6 => Value::IpAddr(None),
            ColumnKind::Nothing => Value::Null,
            ColumnKind::LowCardinality(lc) => lc.0.prototype(),
            ColumnKind::Array(element) => Value::List(
                None,
                Box::new(CollectionType {
                    element: element.prototype(),
                    len: 0,
                }),
            ),
            ColumnKind::Map(kv) => Value::Map(
                None,
                Box::new(MapType {
                    key: kv.0.prototype(),
                    value: kv.1.prototype(),
                }),
            ),
        }
    }

    #[inline(always)]
    fn nanos_to_datetime(nanos: i64) -> Option<PrimitiveDateTime> {
        const DAY_NANOS: i64 = 86_400_000_000_000;
        let days = nanos.div_euclid(DAY_NANOS) as i32;
        let rem = nanos.rem_euclid(DAY_NANOS) as u64;
        let secs = (rem / 1_000_000_000) as u32;
        let ns = (rem % 1_000_000_000) as u32;
        Some(PrimitiveDateTime::new(
            Date::from_julian_day(Self::EPOCH_JULIAN_DAY + days).ok()?,
            Time::from_hms_nano(
                (secs / 3600) as u8,
                ((secs / 60) % 60) as u8,
                (secs % 60) as u8,
                ns,
            )
            .ok()?,
        ))
    }

    /// Hot inline decoder for common scalar column kinds.
    #[inline(always)]
    pub(crate) fn extract<'a>(
        &self,
        batch: &'a ChDBBatch,
        row: usize,
    ) -> Option<Cow<'a, Value<'a>>> {
        if !self.nulls.is_null() && unsafe { *self.nulls.add(row) != 0 } {
            return Some(Cow::Owned(Value::Null));
        }
        let data = self.data;
        let value = unsafe {
            match self.kind {
                ColumnKind::Bool => Value::Boolean(Some(*data.add(row) != 0)),
                ColumnKind::Int32 => {
                    Value::Int32(Some((data as *const i32).add(row).read_unaligned()))
                }
                ColumnKind::Int64 => {
                    Value::Int64(Some((data as *const i64).add(row).read_unaligned()))
                }
                ColumnKind::UInt64 => {
                    Value::UInt64(Some((data as *const u64).add(row).read_unaligned()))
                }
                ColumnKind::Float64 => {
                    Value::Float64(Some((data as *const f64).add(row).read_unaligned()))
                }
                ColumnKind::String(base) => {
                    let &(start, len) = batch.spans.get(base as usize + row)?;
                    let bytes = slice::from_raw_parts(data.add(start as usize), len as usize);
                    match str::from_utf8(bytes) {
                        Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                        Err(_) => Value::Blob(Some(Cow::Borrowed(bytes))),
                    }
                }
                ColumnKind::DateTime64(9, false) => {
                    let nanos = (data as *const i64).add(row).read_unaligned();
                    match Self::nanos_to_datetime(nanos) {
                        Some(dt) => Value::Timestamp(Some(dt)),
                        None => Value::Null,
                    }
                }
                _ => return self.extract_cold(batch, row).map(Cow::Owned),
            }
        };
        Some(Cow::Owned(value))
    }

    /// Cold decoder for less common scalar and nested types.
    /// Returns `Option<Value<'_>>` (different from `extract`) so rustc keeps hot arms in registers.
    #[cold]
    #[inline(never)]
    fn extract_cold<'a>(&self, batch: &'a ChDBBatch, row: usize) -> Option<Value<'a>> {
        let data = self.data;
        unsafe {
            Some(match &self.kind {
                ColumnKind::Int8 => Value::Int8(Some(*data.add(row) as i8)),
                ColumnKind::Int16 => {
                    Value::Int16(Some((data as *const i16).add(row).read_unaligned()))
                }
                ColumnKind::Int128 => Value::Int128(Some(Box::new(
                    (data as *const i128).add(row).read_unaligned(),
                ))),
                ColumnKind::UInt8 => Value::UInt8(Some(*data.add(row))),
                ColumnKind::UInt16 => {
                    Value::UInt16(Some((data as *const u16).add(row).read_unaligned()))
                }
                ColumnKind::UInt32 => {
                    Value::UInt32(Some((data as *const u32).add(row).read_unaligned()))
                }
                ColumnKind::UInt128 => Value::UInt128(Some(Box::new(
                    (data as *const u128).add(row).read_unaligned(),
                ))),
                ColumnKind::Float32 => {
                    Value::Float32(Some((data as *const f32).add(row).read_unaligned()))
                }
                ColumnKind::FixedString(n) => {
                    let len = *n as usize;
                    let bytes = slice::from_raw_parts(data.add(row * len), len);
                    let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
                    match str::from_utf8(&bytes[..end]) {
                        Ok(s) => Value::Varchar(Some(Cow::Borrowed(s))),
                        Err(_) => Value::Blob(Some(Cow::Borrowed(bytes))),
                    }
                }
                ColumnKind::Date => {
                    let days = (data as *const u16).add(row).read_unaligned() as i32;
                    Value::Date(Date::from_julian_day(Self::EPOCH_JULIAN_DAY + days).ok())
                }
                ColumnKind::Date32 => {
                    let days = (data as *const i32).add(row).read_unaligned();
                    Value::Date(
                        Self::EPOCH_JULIAN_DAY
                            .checked_add(days)
                            .and_then(|jd| Date::from_julian_day(jd).ok()),
                    )
                }
                ColumnKind::DateTime(non_utc) => {
                    let secs = (data as *const u32).add(row).read_unaligned() as i64;
                    let Ok(odt) = OffsetDateTime::from_unix_timestamp(secs) else {
                        return Some(Value::Null);
                    };
                    if *non_utc {
                        Value::TimestampWithTimezone(Some(odt))
                    } else {
                        Value::Timestamp(Some(PrimitiveDateTime::new(odt.date(), odt.time())))
                    }
                }
                ColumnKind::DateTime64(scale, non_utc) => {
                    let scale = *scale;
                    let ticks = (data as *const i64).add(row).read_unaligned();
                    let nanos = if scale == 9 {
                        ticks as i128
                    } else if scale < 9 {
                        let factor = 10i64.pow(scale as u32);
                        let secs = ticks.div_euclid(factor);
                        let sub = ticks.rem_euclid(factor) as u64;
                        secs as i128 * 1_000_000_000 + (sub * 10u64.pow(9 - scale as u32)) as i128
                    } else {
                        let scale_down = 10i128.pow((scale - 9) as u32);
                        (ticks as i128).div_euclid(scale_down)
                    };
                    let Ok(odt) = OffsetDateTime::from_unix_timestamp_nanos(nanos) else {
                        return Some(Value::Null);
                    };
                    if *non_utc {
                        Value::TimestampWithTimezone(Some(odt))
                    } else {
                        Value::Timestamp(Some(PrimitiveDateTime::new(odt.date(), odt.time())))
                    }
                }
                ColumnKind::Decimal32(p, s) => {
                    let raw = (data as *const i32).add(row).read_unaligned();
                    Value::Decimal(decimal_from_scaled(raw as i128, *s as u32).ok(), *p, *s)
                }
                ColumnKind::Decimal64(p, s) => {
                    let raw = (data as *const i64).add(row).read_unaligned();
                    Value::Decimal(decimal_from_scaled(raw as i128, *s as u32).ok(), *p, *s)
                }
                ColumnKind::Decimal128(p, s) => {
                    let raw = (data as *const i128).add(row).read_unaligned();
                    Value::Decimal(decimal_from_scaled(raw, *s as u32).ok(), *p, *s)
                }
                ColumnKind::Uuid => {
                    let base = (data as *const u64).add(row * 2);
                    let hi = base.read_unaligned();
                    let lo = base.add(1).read_unaligned();
                    Value::Uuid(Some(Uuid::from_u128(((hi as u128) << 64) | lo as u128)))
                }
                ColumnKind::IPv4 => {
                    let raw = (data as *const u32).add(row).read_unaligned();
                    Value::IpAddr(Some(IpAddr::V4(Ipv4Addr::from(raw))))
                }
                ColumnKind::IPv6 => {
                    let octets = (data as *const [u8; 16]).add(row).read_unaligned();
                    let v6 = Ipv6Addr::from(octets);
                    Value::IpAddr(Some(
                        v6.to_ipv4_mapped()
                            .map(IpAddr::V4)
                            .unwrap_or(IpAddr::V6(v6)),
                    ))
                }
                ColumnKind::Nothing => Value::Null,
                ColumnKind::LowCardinality(lc) => {
                    let (dict, index_shift, nullable) = lc.as_ref();
                    let dict_row = match *index_shift {
                        0 => *data.add(row) as usize,
                        1 => (data as *const u16).add(row).read_unaligned() as usize,
                        2 => (data as *const u32).add(row).read_unaligned() as usize,
                        _ => (data as *const u64).add(row).read_unaligned() as usize,
                    };
                    if *nullable && dict_row == 0 {
                        Value::Null
                    } else {
                        return dict.extract(batch, dict_row).map(Cow::into_owned);
                    }
                }
                ColumnKind::Array(element) => {
                    let (start, end) = Self::offset_range(data, row);
                    let mut values = Vec::with_capacity(end.saturating_sub(start));
                    for i in start..end {
                        values.push(element.extract(batch, i)?.into_owned());
                    }
                    Value::List(
                        Some(values.into_boxed_slice()),
                        Box::new(CollectionType {
                            element: element.prototype(),
                            len: 0,
                        }),
                    )
                }
                ColumnKind::Map(kv) => {
                    let (key, value) = kv.as_ref();
                    let (start, end) = Self::offset_range(data, row);
                    let mut map = HashMap::with_capacity(end.saturating_sub(start));
                    for i in start..end {
                        map.insert(
                            key.extract(batch, i)?.into_owned(),
                            value.extract(batch, i)?.into_owned(),
                        );
                    }
                    Value::Map(
                        Some(Box::new(map)),
                        Box::new(MapType {
                            key: key.prototype(),
                            value: value.prototype(),
                        }),
                    )
                }
                _ => return None,
            })
        }
    }
}
