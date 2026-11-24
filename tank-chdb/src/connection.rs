use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction,
    batch::ChDBBatch,
    ffi::{
        QueryAnalysis, chdb_classify_query_n, chdb_close_conn, chdb_connect,
        chdb_destroy_insert_stream, chdb_destroy_query_result, chdb_result_rows_written,
        chdb_stream_append, chdb_stream_cancel_insert, chdb_stream_done, chdb_stream_insert_error,
        chdb_stream_insert_n, query_error, result_error,
    },
    stream::{ChDBRowStream, ChDBStream},
};
use anyhow::{anyhow, ensure};
use rust_decimal::Decimal;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_int, c_void},
    fmt::Write as _,
    future::{self, Future},
    hint, mem,
    net::IpAddr,
    ptr::{self, NonNull},
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
    handle: NonNull<*mut c_void>,
    pub(crate) batch: Option<Arc<ChDBBatch>>,
    draft: AnyQuery,
    write_buf: Vec<u8>,
}

impl ChDBConnection {
    const WRITER: ChDBSqlWriter = ChDBSqlWriter::chdb();
    const WRITE_BATCH_BYTES: usize = 256 * 1024;
    const EPOCH_JULIAN_DAY: i32 = 2_440_588;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    #[inline(always)]
    pub(crate) fn raw(&self) -> *mut c_void {
        unsafe { *self.handle.as_ptr() }
    }

    #[inline(always)]
    pub(crate) fn classify(&self, sql: &str) -> u32 {
        let mut analysis = QueryAnalysis {
            struct_size: mem::size_of::<QueryAnalysis>() as u32,
            statement_count: 0,
            flags: 0,
            query_class: u32::MAX,
        };
        let state = unsafe {
            chdb_classify_query_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                ptr::null(),
                0,
                &mut analysis,
            )
        };
        if state == 0 {
            analysis.query_class
        } else {
            u32::MAX
        }
    }

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

    #[cold]
    #[inline(never)]
    fn insert_error(handle: *mut c_void, fallback: &str) -> tank_core::Error {
        let err = unsafe { chdb_stream_insert_error(handle) };
        if err.is_null() {
            anyhow!("{fallback}")
        } else {
            anyhow!(
                "ChDB insert stream failed: {}",
                unsafe { CStr::from_ptr(err) }.to_string_lossy()
            )
        }
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
        let mut args = vec![
            CString::new("clickhouse")?,
            CString::new("--allow_experimental_lightweight_delete=1")?,
            CString::new("--join_use_nulls=1")?,
            CString::new("--final=1")?,
        ];
        if let Some(path) = path.as_deref() {
            args.push(CString::new(format!("--path={path}"))?);
        }
        let mut argv: Vec<_> = args.iter().map(|arg| arg.as_ptr().cast_mut()).collect();
        let handle = NonNull::new(unsafe { chdb_connect(argv.len() as c_int, argv.as_mut_ptr()) })
            .ok_or_else(|| anyhow!("Cannot open chDB connection"))
            .with_context(make_context)?;
        let connection = Self {
            handle,
            batch: None,
            draft: AnyQuery::default(),
            write_buf: Vec::new(),
        };
        ensure!(
            !connection.raw().is_null(),
            "Cannot open chDB connection"
        );
        Ok(connection)
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
        struct Guard(*mut c_void, bool);
        unsafe impl Send for Guard {}
        impl Drop for Guard {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    if !self.1 {
                        unsafe { chdb_stream_cancel_insert(self.0) };
                    }
                    unsafe { chdb_destroy_insert_stream(self.0) };
                }
            }
        }

        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<It>::table();
        let columns = E::<It>::columns();
        if columns.is_empty() || !columns.iter().all(Self::supports_row_binary) {
            return self.append_default(entities).await;
        }
        let mut entities = entities.into_iter();
        let mut guard = Guard(ptr::null_mut(), false);
        self.write_buf.clear();
        loop {
            if self.write_buf.len() >= Self::WRITE_BATCH_BYTES {
                if !guard.0.is_null() {
                    let state = unsafe {
                        chdb_stream_append(
                            guard.0,
                            self.write_buf.as_ptr().cast(),
                            self.write_buf.len(),
                        )
                    };
                    if state != 0 {
                        hint::cold_path();
                        return Err(Self::insert_error(guard.0, "ChDB insert stream append failed"));
                    }
                    self.write_buf.clear();
                }
                Self::yield_now().await;
            }
            let Some(entity) = entities.next() else {
                break;
            };
            if guard.0.is_null() {
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
                const FORMAT: &str = "RowBinary";
                let conn = self.raw();
                let sql = self.draft.buffer();
                let handle = unsafe {
                    chdb_stream_insert_n(
                        conn,
                        sql.as_ptr().cast(),
                        sql.len(),
                        FORMAT.as_ptr().cast(),
                        FORMAT.len(),
                    )
                };
                ensure!(!handle.is_null(), "ChDB returned a null insert stream");
                guard.0 = handle;
                if !unsafe { chdb_stream_insert_error(handle) }.is_null() {
                    hint::cold_path();
                    return Err(Self::insert_error(
                        handle,
                        "ChDB insert stream initialization failed",
                    ));
                }
                if self.write_buf.capacity() < Self::WRITE_BATCH_BYTES {
                    self.write_buf.reserve(Self::WRITE_BATCH_BYTES);
                }
            }
            for (value, column) in entity.as_entity().row_values().zip(columns) {
                Self::write_rb_value(&mut self.write_buf, &mut self.draft, column, value)?;
            }
        }
        if guard.0.is_null() {
            return Ok(RowsAffected {
                rows_affected: Some(0),
                last_affected_id: None,
            });
        }
        if !self.write_buf.is_empty() {
            let state = unsafe {
                chdb_stream_append(
                    guard.0,
                    self.write_buf.as_ptr().cast(),
                    self.write_buf.len(),
                )
            };
            self.write_buf.clear();
            if state != 0 {
                hint::cold_path();
                return Err(Self::insert_error(guard.0, "ChDB insert stream append failed"));
            }
        }
        guard.1 = true;
        let result = unsafe { chdb_stream_done(guard.0) };
        if result.is_null() {
            return Err(Self::insert_error(
                guard.0,
                "ChDB insert stream returned null result",
            ));
        }
        let error = unsafe { result_error(result) }.map(query_error);
        let rows_written = unsafe { chdb_result_rows_written(result) };
        unsafe { chdb_destroy_query_result(result) };
        if let Some(error) = error {
            return Err(error);
        }
        Ok(RowsAffected {
            rows_affected: Some(rows_written),
            last_affected_id: None,
        })
    }
}

// Access is serialized by `&mut ChDBConnection`.
unsafe impl Send for ChDBConnection {}

impl Drop for ChDBConnection {
    fn drop(&mut self) {
        unsafe { chdb_close_conn(self.handle.as_ptr()) };
    }
}
