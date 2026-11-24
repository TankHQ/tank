use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction,
    batch::ChDBBatch,
    native::{NativeConnection, NativeInsertStream, ParamBuffer},
    stream::{ChDBRowStream, ChDBStream},
};
use anyhow::anyhow;
use rust_decimal::Decimal;
use std::{
    borrow::Cow,
    fmt::Write as _,
    future::{self, Future},
    hint, mem,
    net::IpAddr,
    sync::Arc,
    task::Poll,
};
use tank_core::{
    AnyQuery, AppendDefault, AsEntity, AsQuery, ColumnDef, Connection, Context, Entity,
    ErrorContext, Executor, Fragment, PrimaryKeyType, QueryResult, Result, Row, RowsAffected,
    SqlCoreWriter, SqlValueWriter, Value, describe_path, separated_by, stream::Stream,
};

/// Wrapper around an embedded chDB connection.
#[derive(Debug)]
pub struct ChDBConnection {
    pub(crate) native: NativeConnection,
    pub(crate) draft: AnyQuery,
    pub(crate) params: ParamBuffer,
    pub(crate) batch: Option<Arc<ChDBBatch>>,
    scratch: AnyQuery,
    write_buf: Vec<u8>,
}

impl ChDBConnection {
    const WRITER: ChDBSqlWriter = ChDBSqlWriter::chdb();
    const WRITE_BATCH_BYTES: usize = 256 * 1024;
    const EPOCH_JULIAN_DAY: i32 = 2_440_588;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    /// Yields once to the async executor so synchronous batches stay cooperative.
    fn yield_now() -> impl Future<Output = ()> + Send {
        let mut yielded = false;
        future::poll_fn(move |cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }

    fn write_escaped_param(dst: &mut Vec<u8>, value: &str) {
        let bytes = value.as_bytes();
        let mut start = 0;
        for (i, &b) in bytes.iter().enumerate() {
            let esc = match b {
                b'\\' => b"\\\\".as_slice(),
                b'\t' => b"\\t".as_slice(),
                b'\n' => b"\\n".as_slice(),
                b'\r' => b"\\r".as_slice(),
                0 => b"\\0".as_slice(),
                _ => continue,
            };
            dst.extend_from_slice(&bytes[start..i]);
            dst.extend_from_slice(esc);
            start = i + 1;
        }
        dst.extend_from_slice(&bytes[start..]);
    }

    /// Rewrites provisional `{pN:Nullable(String)}` markers into `self.draft` and
    /// encodes bound parameters into `self.params` for native `chdb_*_with_params_n`.
    pub(crate) fn prepare_params(&mut self, prepared: &ChDBPrepared) -> Result<()> {
        let markers = prepared.markers();
        let params = prepared.params();
        if markers.len() < params.len() {
            hint::cold_path();
            return Err(anyhow!("Not enough parameter markers for the bound values"));
        }
        self.params.clear();
        let mut name_buf = [0u8; 16];
        let scratch = &mut self.scratch;
        for (i, value) in params.iter().enumerate() {
            let name = Self::format_param_name(&mut name_buf, i);
            self.params.push(name, |dst| {
                if value.is_null() {
                    dst.extend_from_slice(b"\\N");
                    return;
                }
                match value {
                    Value::Varchar(Some(s)) => Self::write_escaped_param(dst, s),
                    Value::Char(Some(c)) => {
                        let mut buf = [0u8; 4];
                        Self::write_escaped_param(dst, c.encode_utf8(&mut buf));
                    }
                    Value::Blob(Some(bytes)) => {
                        dst.reserve(bytes.len() * 2);
                        for &b in bytes.as_ref() {
                            dst.push(Self::HEX[(b >> 4) as usize]);
                            dst.push(Self::HEX[(b & 0xf) as usize]);
                        }
                    }
                    Value::Json(Some(j)) => {
                        scratch.clear();
                        let _ = write!(scratch, "{j}");
                        Self::write_escaped_param(dst, scratch.buffer());
                    }
                    other => {
                        scratch.clear();
                        Self::WRITER.write_value(
                            &mut Context::fragment(Fragment::None),
                            scratch,
                            other,
                        );
                        dst.extend_from_slice(scratch.buffer().as_bytes());
                    }
                }
            });
        }
        self.params.finish();

        self.draft.clear();
        let sql = prepared.sql();
        self.draft.buffer().reserve(sql.len() + 16);
        let mut cursor = 0;
        for (i, marker) in markers.iter().enumerate() {
            if marker.begin > marker.end
                || marker.end > sql.len()
                || marker.begin < cursor
                || !sql.is_char_boundary(marker.begin)
                || !sql.is_char_boundary(marker.end)
            {
                continue;
            }
            self.draft.push_str(&sql[cursor..marker.begin]);
            let _ = write!(self.draft, "{{p{i}:");
            match params.get(i) {
                Some(value) if !value.is_null() => {
                    self.draft.push_str("Nullable(");
                    Self::WRITER.write_column_type(
                        &mut Context::fragment(Fragment::SqlCreateTable),
                        &mut self.draft,
                        value,
                    );
                    self.draft.push(')');
                }
                _ => self.draft.push_str("Nullable(Nothing)"),
            }
            self.draft.push('}');
            cursor = marker.end;
        }
        self.draft.push_str(&sql[cursor..]);
        Ok(())
    }

    fn format_param_name(buf: &mut [u8; 16], index: usize) -> &[u8] {
        buf[0] = b'p';
        let mut n = index;
        let mut digits = [0u8; 15];
        let mut count = 0;
        loop {
            digits[count] = b'0' + (n % 10) as u8;
            count += 1;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        for i in 0..count {
            buf[1 + i] = digits[count - 1 - i];
        }
        &buf[..1 + count]
    }

    fn supports_row_binary(column: &ColumnDef) -> bool {
        if !column.column_type.is_empty() {
            return false;
        }
        matches!(
            column.value,
            Value::Boolean(..)
                | Value::Int8(..)
                | Value::Int16(..)
                | Value::Int32(..)
                | Value::Int64(..)
                | Value::Int128(..)
                | Value::UInt8(..)
                | Value::UInt16(..)
                | Value::UInt32(..)
                | Value::UInt64(..)
                | Value::UInt128(..)
                | Value::Float32(..)
                | Value::Float64(..)
                | Value::Decimal(..)
                | Value::Char(..)
                | Value::Varchar(..)
                | Value::Blob(..)
                | Value::Date(..)
                | Value::Time(..)
                | Value::Timestamp(..)
                | Value::TimestampWithTimezone(..)
                | Value::Interval(..)
                | Value::Uuid(..)
                | Value::IpAddr(..)
                | Value::Json(..)
        )
    }

    #[inline(always)]
    fn write_varuint(dst: &mut Vec<u8>, mut n: usize) {
        if n < 0x80 {
            dst.push(n as u8);
            return;
        }
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            dst.push(b);
            if n == 0 {
                break;
            }
        }
    }

    #[inline(always)]
    fn write_rb_bytes(dst: &mut Vec<u8>, bytes: &[u8]) {
        Self::write_varuint(dst, bytes.len());
        dst.extend_from_slice(bytes);
    }

    fn write_rb_decimal(dst: &mut Vec<u8>, value: Decimal, precision: u8, scale: u8) -> Result<()> {
        let (p, s) = if (precision, scale) == (0, 0) {
            (38u8, 9u8)
        } else {
            (precision, scale)
        };
        let target_scale = s as u32;
        let v_scale = value.scale();
        let mut mantissa = value.mantissa();
        if v_scale < target_scale {
            let factor = 10i128
                .checked_pow(target_scale - v_scale)
                .ok_or_else(|| anyhow!("Decimal scale {target_scale} overflow"))?;
            mantissa = mantissa
                .checked_mul(factor)
                .ok_or_else(|| anyhow!("Decimal {value} overflows Decimal({p},{s})"))?;
        } else if v_scale > target_scale {
            let factor = 10i128
                .checked_pow(v_scale - target_scale)
                .ok_or_else(|| anyhow!("Decimal scale {v_scale} overflow"))?;
            mantissa /= factor;
        }
        match p {
            1..=9 => dst.extend_from_slice(
                &i32::try_from(mantissa)
                    .map_err(|_| anyhow!("Decimal {value} overflows Decimal32({s})"))?
                    .to_le_bytes(),
            ),
            10..=18 => dst.extend_from_slice(
                &i64::try_from(mantissa)
                    .map_err(|_| anyhow!("Decimal {value} overflows Decimal64({s})"))?
                    .to_le_bytes(),
            ),
            19..=38 => dst.extend_from_slice(&mantissa.to_le_bytes()),
            _ => return Err(anyhow!("Unsupported ClickHouse Decimal precision {p}")),
        }
        Ok(())
    }

    #[inline(always)]
    fn write_rb_value(
        dst: &mut Vec<u8>,
        scratch: &mut AnyQuery,
        column: &ColumnDef,
        value: Value<'_>,
    ) -> Result<()> {
        let nullable = column.nullable && column.primary_key == PrimaryKeyType::None;
        if value.is_null() {
            if nullable {
                dst.push(1);
                return Ok(());
            }
            hint::cold_path();
            return Err(anyhow!(
                "Cannot insert NULL into non-nullable column `{}`",
                column.name()
            ));
        }
        if nullable {
            dst.push(0);
        }
        match value {
            Value::Boolean(Some(v)) => {
                dst.push(u8::from(v));
                mem::forget(value);
            }
            Value::Int8(Some(v)) => {
                dst.push(v as u8);
                mem::forget(value);
            }
            Value::Int16(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::Int32(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::Int64(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::Int128(Some(v)) => dst.extend_from_slice(&v.to_le_bytes()),
            Value::UInt8(Some(v)) => {
                dst.push(v);
                mem::forget(value);
            }
            Value::UInt16(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::UInt32(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::UInt64(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::UInt128(Some(v)) => dst.extend_from_slice(&v.to_le_bytes()),
            Value::Float32(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::Float64(Some(v)) => {
                dst.extend_from_slice(&v.to_le_bytes());
                mem::forget(value);
            }
            Value::Varchar(Some(ref v)) => {
                Self::write_rb_bytes(dst, v.as_bytes());
                if matches!(v, Cow::Borrowed(_)) {
                    mem::forget(value);
                }
            }
            Value::Char(Some(v)) => {
                let mut buf = [0u8; 4];
                Self::write_rb_bytes(dst, v.encode_utf8(&mut buf).as_bytes());
                mem::forget(value);
            }
            Value::Timestamp(Some(v)) => {
                let days = (v.date().to_julian_day() - Self::EPOCH_JULIAN_DAY) as i64;
                let (h, m, s, ns) = v.time().as_hms_nano();
                let day_ns = (h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000
                    + ns as i64;
                let nanos = days
                    .checked_mul(86_400_000_000_000)
                    .and_then(|d| d.checked_add(day_ns))
                    .ok_or_else(|| anyhow!("Timestamp {v} out of ClickHouse DateTime64(9) range"))?;
                dst.extend_from_slice(&nanos.to_le_bytes());
                mem::forget(value);
            }
            Value::TimestampWithTimezone(Some(v)) => {
                let nanos = i64::try_from(v.unix_timestamp_nanos())
                    .map_err(|_| anyhow!("Timestamp {v} out of ClickHouse DateTime64(9) range"))?;
                dst.extend_from_slice(&nanos.to_le_bytes());
                mem::forget(value);
            }
            Value::Date(Some(v)) => {
                // ClickHouse `Date` saturates outside 1970-01-01..=2149-06-06 (u16 days).
                let days = (v.to_julian_day() - Self::EPOCH_JULIAN_DAY).clamp(0, u16::MAX as i32)
                    as u16;
                dst.extend_from_slice(&days.to_le_bytes());
                mem::forget(value);
            }
            Value::Decimal(Some(v), ..) => {
                let (p, s) = match column.value {
                    Value::Decimal(_, p, s) => (p, s),
                    _ => (0, 0),
                };
                Self::write_rb_decimal(dst, v, p, s)?;
                mem::forget(value);
            }
            Value::Blob(Some(ref v)) => {
                Self::write_varuint(dst, v.len() * 2);
                dst.reserve(v.len() * 2);
                for &b in v.as_ref() {
                    dst.push(Self::HEX[(b >> 4) as usize]);
                    dst.push(Self::HEX[(b & 0xf) as usize]);
                }
                if matches!(v, Cow::Borrowed(_)) {
                    mem::forget(value);
                }
            }
            Value::Time(Some(v)) => {
                scratch.clear();
                Self::WRITER.write_time(&mut Context::fragment(Fragment::None), scratch, &v);
                Self::write_rb_bytes(dst, scratch.buffer().as_bytes());
                mem::forget(value);
            }
            Value::Interval(Some(v)) => {
                scratch.clear();
                let _ = write!(scratch, "{}ns", v.as_ns());
                Self::write_rb_bytes(dst, scratch.buffer().as_bytes());
                mem::forget(value);
            }
            Value::Uuid(Some(v)) => {
                let raw = v.as_u128();
                dst.extend_from_slice(&((raw >> 64) as u64).to_le_bytes());
                dst.extend_from_slice(&(raw as u64).to_le_bytes());
                mem::forget(value);
            }
            Value::IpAddr(Some(v)) => {
                let octets = match v {
                    IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
                    IpAddr::V6(v6) => v6.octets(),
                };
                dst.extend_from_slice(&octets);
                mem::forget(value);
            }
            Value::Json(Some(ref v)) => {
                scratch.clear();
                let _ = write!(scratch, "{v}");
                Self::write_rb_bytes(dst, scratch.buffer().as_bytes());
            }
            other => {
                hint::cold_path();
                return Err(anyhow!(
                    "Unsupported RowBinary value {other:?} for column `{}`",
                    column.name()
                ));
            }
        }
        Ok(())
    }
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!("While trying to connect to chDB {}", describe_path(&url));
        let path: Option<Cow<'static, str>> = url
            .query_pairs()
            .find_map(|(k, v)| (k.eq_ignore_ascii_case("path") && !v.is_empty()).then_some(v))
            .map(|v| Cow::Owned(v.to_string()))
            .or_else(|| {
                let raw = url.path().trim();
                (!raw.is_empty() && raw != "/")
                    .then(|| Cow::Owned(raw.trim_start_matches('/').to_string()))
            });
        let native = NativeConnection::open(path.as_deref()).with_context(make_context)?;
        Ok(Self {
            native,
            draft: AnyQuery::default(),
            params: ParamBuffer::default(),
            batch: None,
            scratch: AnyQuery::default(),
            write_buf: Vec::new(),
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ChDBTransaction<'_>>> + Send {
        ChDBTransaction::new(self)
    }
}

impl Executor for ChDBConnection {
    type Driver = ChDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    fn do_prepare(&mut self, sql: &str) -> impl Future<Output = Result<ChDBPrepared>> + Send {
        future::ready(Ok(ChDBPrepared::new(sql.to_string())))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ChDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        ChDBStream::new(self, query.as_query(&self.driver()))
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<ChDBDriver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        ChDBRowStream::new(self, query.as_query(&self.driver()))
    }

    async fn append<It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<It>::table();
        let columns = E::<It>::columns();
        if columns.is_empty() || !columns.iter().all(Self::supports_row_binary) {
            return self.append_default(entities).await;
        }
        let mut entities = entities.into_iter();
        let mut stream: Option<NativeInsertStream> = None;
        self.write_buf.clear();
        loop {
            if self.write_buf.len() >= Self::WRITE_BATCH_BYTES {
                if let Some(stream) = stream.as_mut() {
                    stream.append(&self.write_buf)?;
                    self.write_buf.clear();
                }
                Self::yield_now().await;
            }
            let Some(entity) = entities.next() else {
                break;
            };
            if stream.is_none() {
                self.draft.clear();
                let mut context = Context::new(Fragment::SqlInsertInto, false);
                self.draft.push_str("INSERT INTO ");
                Self::WRITER.write_table_ref(&mut context, &mut self.draft, table);
                self.draft.push_str(" (");
                separated_by(
                    &mut self.draft,
                    columns.iter(),
                    |out, col| Self::WRITER.write_identifier(&mut context, out, col.name(), true),
                    ", ",
                );
                self.draft.push(')');
                stream = Some(self.native.insert_stream(self.draft.buffer(), "RowBinary")?);
                if self.write_buf.capacity() < Self::WRITE_BATCH_BYTES {
                    self.write_buf.reserve(Self::WRITE_BATCH_BYTES);
                }
            }
            for (value, column) in entity.as_entity().row_values().zip(columns) {
                Self::write_rb_value(&mut self.write_buf, &mut self.draft, column, value)?;
            }
        }
        let Some(mut stream) = stream else {
            return Ok(RowsAffected {
                rows_affected: Some(0),
                last_affected_id: None,
            });
        };
        if !self.write_buf.is_empty() {
            stream.append(&self.write_buf)?;
            self.write_buf.clear();
        }
        let rows_written = stream.finish()?;
        Ok(RowsAffected {
            rows_affected: Some(rows_written),
            last_affected_id: None,
        })
    }
}
