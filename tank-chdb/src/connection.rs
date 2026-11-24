use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction,
    stream::{ChDBRowStream, ChDBStream},
};
use anyhow::anyhow;
use chdb_rust::{
    InsertOptions,
    arrow::{
        array::{
            ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder,
            Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
            RecordBatch, StringBuilder, TimestampNanosecondBuilder, UInt8Builder, UInt16Builder,
            UInt32Builder, UInt64Builder, make_builder,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
    },
    connection::Connection as RawConnection,
    insert_record_batch_direct,
};
use std::{
    borrow::Cow,
    cmp::{max, min},
    fmt::Write as _,
    future::{self, Future},
    mem,
    net::IpAddr,
    sync::Arc,
    task::Poll,
};
use tank_core::{
    AnyQuery, AppendDefault, AsEntity, AsQuery, ColumnDef, Connection, Context, Entity, Error,
    ErrorContext, Executor, Fragment, PrimaryKeyType, QueryResult, Result, Row, RowLabels,
    RowsAffected, SqlCoreWriter, SqlValueWriter, Value, describe_path, stream::Stream,
};

/// Wrapper around an embedded chDB connection.
#[derive(Debug)]
pub struct ChDBConnection {
    pub(crate) inner: RawConnection,
    pub(crate) labels: RowLabels,
}

impl ChDBConnection {
    const WRITER: ChDBSqlWriter = ChDBSqlWriter::chdb();
    const APPEND_BATCH_ROWS: usize = 262_144;
    const EPOCH_JULIAN_DAY: i32 = 2_440_588;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    fn arrow_field(column: &ColumnDef) -> Option<Field> {
        if !column.column_type.is_empty() {
            return None;
        }
        let nullable = column.nullable && column.primary_key == PrimaryKeyType::None;
        let dt = match column.value {
            Value::Boolean(..) => DataType::Boolean,
            Value::Int8(..) => DataType::Int8,
            Value::Int16(..) => DataType::Int16,
            Value::Int32(..) => DataType::Int32,
            Value::Int64(..) => DataType::Int64,
            Value::UInt8(..) => DataType::UInt8,
            Value::UInt16(..) => DataType::UInt16,
            Value::UInt32(..) => DataType::UInt32,
            Value::UInt64(..) => DataType::UInt64,
            Value::Float32(..) => DataType::Float32,
            Value::Float64(..) => DataType::Float64,
            Value::Decimal(_, p, s) => {
                let (p, s) = if (p, s) == (0, 0) { (38, 9) } else { (p, s) };
                DataType::Decimal128(p, s as i8)
            }
            Value::Char(..)
            | Value::Varchar(..)
            | Value::Blob(..)
            | Value::Time(..)
            | Value::Interval(..)
            | Value::Uuid(..)
            | Value::IpAddr(..)
            | Value::Json(..) => DataType::Utf8,
            Value::Date(..) => DataType::Date32,
            Value::Timestamp(..) | Value::TimestampWithTimezone(..) => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            _ => return None,
        };
        Some(Field::new(column.name(), dt, nullable))
    }

    #[inline(always)]
    fn push_arrow(
        builder: &mut dyn ArrayBuilder,
        data_type: &DataType,
        value: Value<'_>,
        scratch: &mut AnyQuery,
    ) -> Result<()> {
        macro_rules! push_prim {
            ($b:ty, $v:expr) => {{
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<$b>()
                    .ok_or_else(|| anyhow!("Mismatched Arrow builder for {data_type:?}"))?;
                b.append_value($v);
            }};
        }
        'simple_value: {
            match value {
                Value::Boolean(Some(n)) if matches!(data_type, DataType::Boolean) => {
                    push_prim!(BooleanBuilder, n);
                }
                Value::Int32(Some(n)) if matches!(data_type, DataType::Int32) => {
                    push_prim!(Int32Builder, n);
                }
                Value::Int64(Some(n)) if matches!(data_type, DataType::Int64) => {
                    push_prim!(Int64Builder, n);
                }
                Value::Float64(Some(n)) if matches!(data_type, DataType::Float64) => {
                    push_prim!(Float64Builder, n);
                }
                Value::Varchar(ref v) if matches!(data_type, DataType::Utf8) => {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .ok_or_else(|| anyhow!("Mismatched Arrow builder for Utf8"))?;
                    b.append_option(v.as_deref());
                    if matches!(v, Some(Cow::Owned(_))) {
                        break 'simple_value;
                    }
                }
                Value::Timestamp(Some(ts))
                    if matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _)) =>
                {
                    let b = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampNanosecondBuilder>()
                        .ok_or_else(|| anyhow!("Mismatched Arrow builder for Timestamp"))?;
                    let days = (ts.date().to_julian_day() - Self::EPOCH_JULIAN_DAY) as i64;
                    if (days as u64).wrapping_add(106_750) > 213_500 {
                        return Err(anyhow!(
                            "Timestamp {ts} out of ClickHouse DateTime64(9) range"
                        ));
                    }
                    let (h, m, s, ns) = ts.time().as_hms_nano();
                    let secs = days * 86_400 + h as i64 * 3600 + m as i64 * 60 + s as i64;
                    b.append_value(secs * 1_000_000_000 + ns as i64);
                }
                other => {
                    let res = push_other(builder, data_type, other, scratch);
                    #[inline(never)]
                    fn push_other(
                        builder: &mut dyn ArrayBuilder,
                        data_type: &DataType,
                        value: Value<'_>,
                        scratch: &mut AnyQuery,
                    ) -> Result<()> {
                        if value.is_null() {
                            macro_rules! push_null {
                                ($b:ty) => {{
                                    builder
                                        .as_any_mut()
                                        .downcast_mut::<$b>()
                                        .ok_or_else(|| {
                                            anyhow!("Mismatched Arrow builder for {data_type:?}")
                                        })?
                                        .append_null();
                                }};
                            }
                            match data_type {
                                DataType::Boolean => push_null!(BooleanBuilder),
                                DataType::Int8 => push_null!(Int8Builder),
                                DataType::Int16 => push_null!(Int16Builder),
                                DataType::Int32 => push_null!(Int32Builder),
                                DataType::Int64 => push_null!(Int64Builder),
                                DataType::UInt8 => push_null!(UInt8Builder),
                                DataType::UInt16 => push_null!(UInt16Builder),
                                DataType::UInt32 => push_null!(UInt32Builder),
                                DataType::UInt64 => push_null!(UInt64Builder),
                                DataType::Float32 => push_null!(Float32Builder),
                                DataType::Float64 => push_null!(Float64Builder),
                                DataType::Decimal128(..) => push_null!(Decimal128Builder),
                                DataType::Utf8 => push_null!(StringBuilder),
                                DataType::Date32 => push_null!(Date32Builder),
                                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                                    push_null!(TimestampNanosecondBuilder)
                                }
                                _ => {
                                    return Err(anyhow!(
                                        "Unsupported Arrow null type {data_type:?}"
                                    ));
                                }
                            }
                            return Ok(());
                        }
                        macro_rules! push_val {
                            ($b:ty, $v:expr) => {{
                                builder
                                    .as_any_mut()
                                    .downcast_mut::<$b>()
                                    .ok_or_else(|| {
                                        anyhow!("Mismatched Arrow builder for {data_type:?}")
                                    })?
                                    .append_value($v);
                            }};
                        }
                        match (data_type, value) {
                            (DataType::Int8, Value::Int8(Some(v))) => push_val!(Int8Builder, v),
                            (DataType::Int16, Value::Int16(Some(v))) => push_val!(Int16Builder, v),
                            (DataType::UInt8, Value::UInt8(Some(v))) => push_val!(UInt8Builder, v),
                            (DataType::UInt16, Value::UInt16(Some(v))) => {
                                push_val!(UInt16Builder, v)
                            }
                            (DataType::UInt32, Value::UInt32(Some(v))) => {
                                push_val!(UInt32Builder, v)
                            }
                            (DataType::UInt64, Value::UInt64(Some(v))) => {
                                push_val!(UInt64Builder, v)
                            }
                            (DataType::Float32, Value::Float32(Some(v))) => {
                                push_val!(Float32Builder, v)
                            }
                            (DataType::Decimal128(p, s), Value::Decimal(Some(v), ..)) => {
                                let target = max(*s, 0) as u32;
                                let v_scale = v.scale();
                                let mut mantissa = v.mantissa();
                                if v_scale < target {
                                    let factor =
                                        10i128.checked_pow(target - v_scale).ok_or_else(|| {
                                            anyhow!("Decimal scale {target} overflow")
                                        })?;
                                    mantissa = mantissa.checked_mul(factor).ok_or_else(|| {
                                        anyhow!("Decimal {v} overflows Decimal({p},{s})")
                                    })?;
                                } else if v_scale > target {
                                    let factor =
                                        10i128.checked_pow(v_scale - target).ok_or_else(|| {
                                            anyhow!("Decimal scale {v_scale} overflow")
                                        })?;
                                    mantissa /= factor;
                                }
                                builder
                                    .as_any_mut()
                                    .downcast_mut::<Decimal128Builder>()
                                    .ok_or_else(|| {
                                        anyhow!("Mismatched Arrow builder for Decimal128")
                                    })?
                                    .append_value(mantissa);
                            }
                            (DataType::Date32, Value::Date(Some(v))) => {
                                let days = (v.to_julian_day() - ChDBConnection::EPOCH_JULIAN_DAY)
                                    .clamp(0, u16::MAX as i32);
                                builder
                                    .as_any_mut()
                                    .downcast_mut::<Date32Builder>()
                                    .ok_or_else(|| anyhow!("Mismatched Arrow builder for Date32"))?
                                    .append_value(days);
                            }
                            (
                                DataType::Timestamp(TimeUnit::Nanosecond, _),
                                Value::TimestampWithTimezone(Some(v)),
                            ) => {
                                let nanos =
                                    i64::try_from(v.unix_timestamp_nanos()).map_err(|_| {
                                        anyhow!(
                                            "Timestamp {v} out of ClickHouse DateTime64(9) range"
                                        )
                                    })?;
                                builder
                                    .as_any_mut()
                                    .downcast_mut::<TimestampNanosecondBuilder>()
                                    .ok_or_else(|| {
                                        anyhow!("Mismatched Arrow builder for Timestamp")
                                    })?
                                    .append_value(nanos);
                            }
                            (DataType::Utf8, other) => {
                                let b = builder
                                    .as_any_mut()
                                    .downcast_mut::<StringBuilder>()
                                    .ok_or_else(|| anyhow!("Mismatched Arrow builder for Utf8"))?;
                                match other {
                                    Value::Char(Some(c)) => {
                                        let mut buf = [0u8; 4];
                                        b.append_value(c.encode_utf8(&mut buf));
                                    }
                                    Value::Blob(Some(v)) => {
                                        scratch.clear();
                                        for &byte in v.as_ref() {
                                            scratch.push(
                                                ChDBConnection::HEX[(byte >> 4) as usize] as char,
                                            );
                                            scratch.push(
                                                ChDBConnection::HEX[(byte & 0xf) as usize] as char,
                                            );
                                        }
                                        b.append_value(scratch.buffer());
                                    }
                                    Value::Time(Some(v)) => {
                                        scratch.clear();
                                        ChDBConnection::WRITER.write_time(
                                            &mut Context::fragment(Fragment::None),
                                            scratch,
                                            &v,
                                        );
                                        b.append_value(scratch.buffer());
                                    }
                                    Value::Interval(Some(v)) => {
                                        scratch.clear();
                                        let _ = write!(scratch, "{}ns", v.as_ns());
                                        b.append_value(scratch.buffer());
                                    }
                                    Value::Uuid(Some(v)) => {
                                        let mut buf = [0u8; 36];
                                        b.append_value(v.hyphenated().encode_lower(&mut buf));
                                    }
                                    Value::IpAddr(Some(v)) => {
                                        scratch.clear();
                                        match v {
                                            IpAddr::V4(v4) => {
                                                let _ = write!(scratch, "{v4}");
                                            }
                                            IpAddr::V6(v6) => {
                                                let _ = write!(scratch, "{v6}");
                                            }
                                        }
                                        b.append_value(scratch.buffer());
                                    }
                                    Value::Json(Some(v)) => {
                                        scratch.clear();
                                        let _ = write!(scratch, "{v}");
                                        b.append_value(scratch.buffer());
                                    }
                                    other => {
                                        return Err(anyhow!(
                                            "Unsupported Arrow append value {other:?}"
                                        ));
                                    }
                                }
                            }
                            (_, other) => {
                                return Err(anyhow!(
                                    "Unsupported Arrow append value {other:?} for {data_type:?}"
                                ));
                            }
                        }
                        Ok(())
                    }
                    res?;
                    break 'simple_value;
                }
            }
            // Avoid an opaque Value::drop call for simple values that do not allocate memory.
            mem::forget(value);
        }
        Ok(())
    }
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!("While trying to connect to chDB {}", describe_path(&url));
        let path: Option<String> = url
            .query_pairs()
            .find_map(|(k, v)| (k.eq_ignore_ascii_case("path") && !v.is_empty()).then_some(v))
            .map(|v| format!("--path={v}"))
            .or_else(|| {
                let raw = url.path().trim();
                (!raw.is_empty() && raw != "/")
                    .then(|| format!("--path={}", raw.trim_start_matches('/')))
            });
        let mut args = vec![
            "--allow_experimental_lightweight_delete=1",
            "--join_use_nulls=1",
            "--final=1",
        ];
        if let Some(path) = path.as_deref() {
            args.push(path);
        }
        let inner = RawConnection::open(&args)
            .map_err(Error::new)
            .with_context(make_context)?;
        Ok(Self {
            inner,
            labels: RowLabels::default(),
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
        let mut entities = entities.into_iter();
        let fields: Option<Vec<Field>> = (!columns.is_empty())
            .then(|| columns.iter().map(Self::arrow_field).collect())
            .flatten();
        let Some(fields) = fields else {
            return self.append_default(entities).await;
        };
        let cap = min(max(entities.size_hint().0, 64), Self::APPEND_BATCH_ROWS);
        let mut builders: Vec<Box<dyn ArrayBuilder>> = fields
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8 => {
                    Box::new(StringBuilder::with_capacity(cap, cap.saturating_mul(32)))
                        as Box<dyn ArrayBuilder>
                }
                dt => make_builder(dt, cap),
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mut dest = AnyQuery::default();
        Self::WRITER.write_table_ref(
            &mut Context::new(Fragment::SqlInsertInto, false),
            &mut dest,
            table,
        );
        let dest_table = dest.buffer().to_owned();
        let insert_opts = InsertOptions {
            max_threads: Some(1),
            ..Default::default()
        };
        let mut scratch = AnyQuery::default();
        let mut total = 0u64;
        let mut batch_rows = 0usize;

        loop {
            if batch_rows >= Self::APPEND_BATCH_ROWS {
                let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(Error::new)?;
                insert_record_batch_direct(&self.inner, &dest_table, batch, insert_opts.clone())
                    .map_err(Error::new)?;
                total += batch_rows as u64;
                batch_rows = 0;
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
                .await;
            }
            let Some(entity) = entities.next() else {
                break;
            };
            for ((value, builder), field) in entity
                .as_entity()
                .row_values()
                .zip(&mut builders)
                .zip(schema.fields())
            {
                Self::push_arrow(builder.as_mut(), field.data_type(), value, &mut scratch)?;
            }
            batch_rows += 1;
        }
        if batch_rows > 0 {
            let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
            let batch = RecordBatch::try_new(schema, arrays).map_err(Error::new)?;
            insert_record_batch_direct(&self.inner, &dest_table, batch, insert_opts)
                .map_err(Error::new)?;
            total += batch_rows as u64;
        }
        Ok(RowsAffected {
            rows_affected: Some(total),
            last_affected_id: None,
        })
    }
}
