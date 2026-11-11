use crate::{
    DuckDBPrepared, DuckDBTransaction, ValueWrap,
    driver::DuckDBDriver,
    ensure_rc,
    streaming::{Batch, DuckDBStream},
    util::CBox,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_void},
    future::{self, Future},
    hint, mem, ptr,
    str::FromStr,
    sync::{Arc, LazyLock},
};
use tank_core::{
    AsEntity, AsQuery, Connection, Driver, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, Result, Row, RowsAffected, SqlWriter, Value, as_c_string, describe_path,
    error_message_from_ptr, log_error, log_warn, stream::Stream, truncate_long,
};

/// Wrapper around a DuckDB `duckdb_connection` pointer used by the DuckDB driver.
#[derive(Debug)]
pub struct DuckDBConnection {
    // Drop order: release cached chunks and disconnect before closing the database handle.
    pub(crate) batch: Option<Arc<Batch>>,
    pub(crate) connection: CBox<duckdb_connection>,
    _database: CBox<duckdb_database>,
}

/// A data chunk column filled by the appender, with its buffers cached for the current chunk.
struct AppendColumn {
    vector: duckdb_vector,
    data: *mut c_void,
    validity: *mut u64,
}

impl AppendColumn {
    #[inline(always)]
    unsafe fn reset(&mut self) {
        self.data = unsafe { duckdb_vector_get_data(self.vector) };
        self.validity = ptr::null_mut();
    }

    #[inline(always)]
    unsafe fn write<T>(&self, row: usize, value: T) {
        unsafe { self.data.cast::<T>().add(row).write(value) }
    }

    #[inline(always)]
    unsafe fn write_bytes(&self, row: usize, bytes: &[u8]) {
        unsafe {
            if bytes.len() <= 12 {
                let mut raw: duckdb_string_t = mem::zeroed();
                raw.value.inlined.length = bytes.len() as u32;
                ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    raw.value.inlined.inlined.as_mut_ptr() as *mut u8,
                    bytes.len(),
                );
                self.write(row, raw);
            } else {
                duckdb_unsafe_vector_assign_string_element_len(
                    self.vector,
                    row as _,
                    bytes.as_ptr() as *const c_char,
                    bytes.len() as _,
                );
            }
        }
    }

    #[inline(always)]
    unsafe fn set_null(&mut self, row: usize) {
        unsafe {
            if self.validity.is_null() {
                duckdb_vector_ensure_validity_writable(self.vector);
                self.validity = duckdb_vector_get_validity(self.vector);
            }
            *self.validity.add(row >> 6) &= !(1 << (row & 63));
        }
    }
}

impl DuckDBConnection {
    #[inline(always)]
    unsafe fn check_appender(appender: duckdb_appender, rc: duckdb_state) -> Result<()> {
        if rc == duckdb_state_DuckDBSuccess {
            Ok(())
        } else {
            hint::cold_path();
            Err(Error::msg(
                error_message_from_ptr(&unsafe { duckdb_appender_error(appender) }).into_owned(),
            ))
            .inspect_err(log_error)
        }
    }
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        static DATABASE_CACHE: LazyLock<CBox<duckdb_instance_cache>> =
            LazyLock::new(|| CBox(unsafe { duckdb_create_instance_cache() }));
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!("While trying to connect to DuckDB {}", describe_path(&url));
        let mut config = CBox::<duckdb_config>::null();
        ensure_rc!(
            unsafe { duckdb_create_config(&mut *config) },
            anyhow!("Cannot allocate the duckdb_config object").context(make_context())
        );
        let mut path = CString::from_str(&format!(
            "{}{}",
            url.host_str()
                .map_or(Default::default(), |host| format!("{host}/")),
            url.path()
        ))
        .with_context(make_context)?;
        for (k, v) in url.query_pairs() {
            let rc = unsafe {
                match &*k {
                    "mode" => {
                        if v == "memory" {
                            path = CString::from_str(":memory:")?;
                            continue;
                        }
                        duckdb_set_config(
                            *config,
                            c"access_mode".as_ptr(),
                            match &*v {
                                "ro" => c"READ_ONLY",
                                "rw" | "rwc" => c"READ_WRITE",
                                _ => {
                                    let error = anyhow!(
                                        "Unknown value {v:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`"
                                    );
                                    return Err(error).inspect_err(log_warn);
                                }
                            }
                            .as_ptr(),
                        )
                    }
                    _ => duckdb_set_config(
                        *config,
                        as_c_string(k.as_bytes()).as_ptr(),
                        as_c_string(v.as_bytes()).as_ptr(),
                    ),
                }
            };
            ensure_rc!(rc, anyhow!("Error while setting config `{k}={v}`"));
        }
        let mut database = CBox::<duckdb_database>::null();
        let mut error = CBox::<*mut c_char>::null();
        let mut connection = CBox::<duckdb_connection>::null();
        unsafe {
            ensure_rc!(
                duckdb_get_or_create_from_cache(
                    **DATABASE_CACHE,
                    path.as_ptr(),
                    &mut *database,
                    *config,
                    &mut *error,
                ),
                Error::msg(
                    CStr::from_ptr(*error)
                        .to_str()
                        .context("While reading the error from `duckdb_get_or_create_from_cache`")
                        .inspect_err(log_error)?
                ),
            );
            ensure_rc!(
                duckdb_connect(*database, &mut *connection),
                anyhow!("Failed to connect to database url `{url}`"),
            );
        }
        Ok(DuckDBConnection {
            batch: None,
            connection,
            _database: database,
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<DuckDBTransaction<'_>>> + Send {
        DuckDBTransaction::new(self)
    }
}

impl Executor for DuckDBConnection {
    type Driver = DuckDBDriver;

    fn do_prepare(
        &mut self,
        sql: String,
    ) -> impl Future<Output = Result<Query<DuckDBDriver>>> + Send {
        future::ready(
            (|| unsafe {
                let c_sql = CString::new(sql.as_bytes())
                    .context("Could not convert `String` to `CString`")?;
                let mut prepared = CBox::null();
                ensure_rc!(
                    duckdb_prepare(*self.connection, c_sql.as_ptr(), &mut *prepared),
                    Error::msg(
                        error_message_from_ptr(&duckdb_prepare_error(*prepared)).into_owned(),
                    )
                );
                Ok(DuckDBPrepared::new(prepared).into())
            })()
            .with_context(|| format!("While preparing the query:\n{}", truncate_long!(sql)))
            .inspect_err(log_error),
        )
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<DuckDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        DuckDBStream::new(self, query.as_query())
    }

    fn append<It>(&mut self, rows: It) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<It>::table();
        let columns = E::<It>::columns();
        let delimiter = self
            .driver()
            .sql_writer()
            .separator()
            .chars()
            .next()
            .unwrap_or('.');
        future::ready((|| unsafe {
            let mut appender = CBox::null();
            let rc = match table.schema.rsplit_once(delimiter) {
                Some((catalog, schema)) => duckdb_appender_create_ext(
                    *self.connection,
                    as_c_string(catalog).as_ptr(),
                    as_c_string(schema).as_ptr(),
                    as_c_string(&*table.name).as_ptr(),
                    &mut *appender,
                ),
                None if table.schema.is_empty() => duckdb_appender_create(
                    *self.connection,
                    ptr::null(),
                    as_c_string(&*table.name).as_ptr(),
                    &mut *appender,
                ),
                None => duckdb_appender_create(
                    *self.connection,
                    as_c_string(&*table.schema).as_ptr(),
                    as_c_string(&*table.name).as_ptr(),
                    &mut *appender,
                ),
            };
            Self::check_appender(*appender, rc)
                .context("While creating the `duckdb_appender` object")?;
            for column in columns {
                Self::check_appender(
                    *appender,
                    duckdb_appender_add_column(*appender, as_c_string(column.name()).as_ptr()),
                )?;
            }
            let mut rows_affected = 0;
            if !columns.is_empty()
                && columns.iter().all(|c| {
                    matches!(
                        c.value,
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
                            | Value::Decimal(_, 1.., _)
                            | Value::Char(..)
                            | Value::Varchar(..)
                            | Value::Blob(..)
                            | Value::Date(..)
                            | Value::Time(..)
                            | Value::Timestamp(..)
                            | Value::TimestampWithTimezone(..)
                            | Value::Interval(..)
                            | Value::Uuid(..)
                    )
                })
            {
                let mut types = columns
                    .iter()
                    .map(|c| CBox::from(ValueWrap::from(&c.value)))
                    .collect::<Box<[_]>>();
                let chunk = CBox(duckdb_create_data_chunk(
                    types.as_mut_ptr() as _,
                    types.len() as _,
                ));
                if chunk.is_null() {
                    return Err(anyhow!("Could not create a DuckDB data chunk"));
                }
                let batch_size = duckdb_vector_size() as usize;
                let mut rows = rows.into_iter();
                let mut columns = (0..types.len())
                    .map(|i| AppendColumn {
                        vector: duckdb_data_chunk_get_vector(*chunk, i as _),
                        data: ptr::null_mut(),
                        validity: ptr::null_mut(),
                    })
                    .collect::<Box<[_]>>();
                loop {
                    duckdb_data_chunk_reset(*chunk);
                    for column in &mut columns {
                        column.reset();
                    }
                    let mut len = 0;
                    while len < batch_size {
                        let Some(entity) = rows.next() else {
                            break;
                        };
                        for (value, column) in entity.as_entity().row_values().zip(&mut columns) {
                            match value {
                                Value::Boolean(Some(v)) => column.write(len, v),
                                Value::Int8(Some(v)) => column.write(len, v),
                                Value::Int16(Some(v)) => column.write(len, v),
                                Value::Int32(Some(v)) => column.write(len, v),
                                Value::Int64(Some(v)) => column.write(len, v),
                                Value::Int128(Some(v)) => {
                                    column.write(len, ValueWrap::from_i128(*v))
                                }
                                Value::UInt8(Some(v)) => column.write(len, v),
                                Value::UInt16(Some(v)) => column.write(len, v),
                                Value::UInt32(Some(v)) => column.write(len, v),
                                Value::UInt64(Some(v)) => column.write(len, v),
                                Value::UInt128(Some(v)) => {
                                    column.write(len, ValueWrap::from_u128(*v))
                                }
                                Value::Float32(Some(v)) => column.write(len, v),
                                Value::Float64(Some(v)) => column.write(len, v),
                                Value::Decimal(Some(v), width, scale) => {
                                    let raw = ValueWrap::from_decimal(&v, width, scale).value;
                                    match width {
                                        ..=4 => column.write(len, raw.lower as i16),
                                        ..=9 => column.write(len, raw.lower as i32),
                                        ..=18 => column.write(len, raw.lower as i64),
                                        _ => column.write(len, raw),
                                    }
                                }
                                Value::Char(Some(v)) => {
                                    column.write_bytes(len, v.encode_utf8(&mut [0; 4]).as_bytes())
                                }
                                Value::Varchar(Some(v)) => column.write_bytes(len, v.as_bytes()),
                                Value::Blob(Some(v)) => column.write_bytes(len, &v),
                                Value::Date(Some(v)) => column.write(len, ValueWrap::from_date(&v)),
                                Value::Time(Some(v)) => column.write(len, ValueWrap::from_time(&v)),
                                Value::Timestamp(Some(v)) => {
                                    column.write(len, ValueWrap::from_timestamp(&v))
                                }
                                Value::TimestampWithTimezone(Some(v)) => {
                                    column.write(len, ValueWrap::from_timestamptz(&v))
                                }
                                Value::Interval(Some(v)) => {
                                    column.write(len, ValueWrap::from_interval(&v))
                                }
                                Value::Uuid(Some(v)) => {
                                    let (upper, lower) = v.as_u64_pair();
                                    column.write(
                                        len,
                                        duckdb_hugeint {
                                            lower,
                                            upper: (upper ^ (1 << 63)) as i64,
                                        },
                                    )
                                }
                                _ => column.set_null(len),
                            }
                        }
                        len += 1;
                    }
                    if len == 0 {
                        break;
                    }
                    duckdb_data_chunk_set_size(*chunk, len as _);
                    Self::check_appender(*appender, duckdb_append_data_chunk(*appender, *chunk))?;
                    rows_affected += len as u64;
                }
            } else {
                for entity in rows {
                    for value in entity.as_entity().row_values() {
                        Self::check_appender(
                            *appender,
                            duckdb_append_value(
                                *appender,
                                *CBox::try_from(ValueWrap::from(value))?,
                            ),
                        )?;
                    }
                    Self::check_appender(*appender, duckdb_appender_end_row(*appender))?;
                    rows_affected += 1;
                }
            }
            Self::check_appender(*appender, duckdb_appender_close(*appender))
                .context("While closing the `duckdb_appender` object")?;
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            })
        })())
    }
}
