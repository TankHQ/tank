use crate::{
    DuckDBPrepared, DuckDBTransaction, ValueWrap, cbox::CBox, driver::DuckDBDriver,
    streaming::DuckDBStream,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_void},
    future::{self, Future},
    mem, ptr,
    str::FromStr,
    sync::LazyLock,
};
use tank_core::{
    AsEntity, AsQuery, Connection, Driver, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, Result, Row, RowsAffected, SqlWriter, Value, as_c_string, describe_path,
    error_message_from_ptr, stream::Stream, truncate_long,
};

/// Wrapper around a DuckDB `duckdb_connection` pointer used by the DuckDB driver.
#[derive(Debug)]
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_connection>,
}

impl DuckDBConnection {
    #[inline(always)]
    unsafe fn write<T>(data: *mut c_void, row: usize, value: T) {
        unsafe { data.cast::<T>().add(row).write(value) }
    }

    #[inline(always)]
    unsafe fn check_appender(appender: duckdb_appender, rc: duckdb_state) -> Result<()> {
        if rc == duckdb_state_DuckDBSuccess {
            return Ok(());
        }
        let error = Error::msg(
            error_message_from_ptr(&unsafe { duckdb_appender_error(appender) }).into_owned(),
        );
        log::error!("{error:#}");
        Err(error)
    }
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        static DATABASE_CACHE: LazyLock<CBox<duckdb_instance_cache>> =
            LazyLock::new(|| CBox(unsafe { duckdb_create_instance_cache() }));
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to DuckDB {}",
                describe_path::<DuckDBDriver>(&url)
            )
        };
        let mut config = CBox::null();
        unsafe {
            if duckdb_create_config(&mut *config) != duckdb_state_DuckDBSuccess {
                let error =
                    anyhow!("Cannot allocate the duckdb_config object").context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
        }
        let mut path = CString::from_str(&format!(
            "{}{}",
            url.host_str()
                .map_or(Default::default(), |host| format!("{host}/")),
            url.path()
        ))
        .with_context(make_context)?;
        for (key, value) in url.query_pairs() {
            let rc = unsafe {
                match &*key {
                    "mode" => {
                        if value == "memory" {
                            path = CString::from_str(":memory:")?;
                            continue;
                        }
                        duckdb_set_config(
                            *config,
                            c"access_mode".as_ptr(),
                            match &*value {
                                "ro" => c"READ_ONLY",
                                "rw" | "rwc" => c"READ_WRITE",
                                _ => {
                                    let error = anyhow!(
                                        "Unknown value {value:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`"
                                    );
                                    log::warn!("{error:#}");
                                    return Err(error);
                                }
                            }
                            .as_ptr(),
                        )
                    }
                    _ => duckdb_set_config(
                        *config,
                        as_c_string(&*key).as_ptr(),
                        as_c_string(&*value).as_ptr(),
                    ),
                }
            };
            if rc != duckdb_state_DuckDBSuccess {
                let error = anyhow!("Error while setting config `{key}={value}`");
                log::warn!("{error:#}");
                return Err(error);
            }
        }
        let mut database = ptr::null_mut();
        let mut error = CBox::null();
        let mut connection = CBox::null();
        unsafe {
            if duckdb_get_or_create_from_cache(
                **DATABASE_CACHE,
                path.as_ptr(),
                &mut database,
                *config,
                &mut *error,
            ) != duckdb_state_DuckDBSuccess
            {
                return Err(Error::msg(
                    CStr::from_ptr(*error)
                        .to_str()
                        .context("While reading the error from `duckdb_get_or_create_from_cache`")?
                        .to_owned(),
                ));
            }
            if duckdb_connect(database, &mut *connection) != duckdb_state_DuckDBSuccess {
                let error = anyhow!("Failed to connect to database url `{url}`");
                log::error!("{error:#}");
                return Err(error);
            }
        }
        Ok(DuckDBConnection { connection })
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
                if duckdb_prepare(*self.connection, c_sql.as_ptr(), &mut *prepared)
                    != duckdb_state_DuckDBSuccess
                {
                    return Err(Error::msg(
                        error_message_from_ptr(&duckdb_prepare_error(*prepared)).into_owned(),
                    ));
                }
                Ok(DuckDBPrepared::new(prepared).into())
            })()
            .with_context(|| format!("While preparing the query:\n{}", truncate_long!(sql)))
            .inspect_err(|error| log::error!("{error:#}")),
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
        future::ready((|| unsafe {
            let table = E::<It>::table();
            let delimiter = self
                .driver()
                .sql_writer()
                .separator()
                .chars()
                .next()
                .unwrap_or('.');
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
            let columns = E::<It>::columns();
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
                let mut cells = (0..types.len())
                    .map(|i| {
                        (
                            duckdb_data_chunk_get_vector(*chunk, i as _),
                            ptr::null_mut(),
                        )
                    })
                    .collect::<Box<[_]>>();
                loop {
                    duckdb_data_chunk_reset(*chunk);
                    for (vector, data) in &mut cells {
                        *data = duckdb_vector_get_data(*vector);
                    }
                    let mut len = 0;
                    while len < batch_size {
                        let Some(entity) = rows.next() else {
                            break;
                        };
                        for (val, &(vector, data)) in
                            entity.as_entity().row_values().into_iter().zip(&cells)
                        {
                            match val {
                                Value::Boolean(Some(v)) => Self::write(data, len, v),
                                Value::Int8(Some(v)) => Self::write(data, len, v),
                                Value::Int16(Some(v)) => Self::write(data, len, v),
                                Value::Int32(Some(v)) => Self::write(data, len, v),
                                Value::Int64(Some(v)) => Self::write(data, len, v),
                                Value::Int128(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_i128(*v))
                                }
                                Value::UInt8(Some(v)) => Self::write(data, len, v),
                                Value::UInt16(Some(v)) => Self::write(data, len, v),
                                Value::UInt32(Some(v)) => Self::write(data, len, v),
                                Value::UInt64(Some(v)) => Self::write(data, len, v),
                                Value::UInt128(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_u128(*v))
                                }
                                Value::Float32(Some(v)) => Self::write(data, len, v),
                                Value::Float64(Some(v)) => Self::write(data, len, v),
                                Value::Decimal(Some(v), width, scale) => {
                                    let raw = ValueWrap::from_decimal(&v, width, scale).value;
                                    match width {
                                        ..=4 => Self::write(data, len, raw.lower as i16),
                                        ..=9 => Self::write(data, len, raw.lower as i32),
                                        ..=18 => Self::write(data, len, raw.lower as i64),
                                        _ => Self::write(data, len, raw),
                                    }
                                }
                                Value::Char(Some(..))
                                | Value::Varchar(Some(..))
                                | Value::Blob(Some(..)) => {
                                    let mut buf = [0u8; 4];
                                    let src = match &val {
                                        Value::Char(Some(v)) => v.encode_utf8(&mut buf).as_bytes(),
                                        Value::Varchar(Some(v)) => v.as_bytes(),
                                        Value::Blob(Some(v)) => v.as_ref(),
                                        _ => unreachable!(),
                                    };
                                    if src.len() <= 12 {
                                        let mut raw: duckdb_string_t = mem::zeroed();
                                        raw.value.inlined.length = src.len() as u32;
                                        ptr::copy_nonoverlapping(
                                            src.as_ptr(),
                                            raw.value.inlined.inlined.as_mut_ptr() as *mut u8,
                                            src.len(),
                                        );
                                        Self::write(data, len, raw);
                                    } else {
                                        duckdb_unsafe_vector_assign_string_element_len(
                                            vector,
                                            len as _,
                                            src.as_ptr() as *const c_char,
                                            src.len() as _,
                                        );
                                    }
                                }
                                Value::Date(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_date(&v))
                                }
                                Value::Time(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_time(&v))
                                }
                                Value::Timestamp(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_timestamp(&v))
                                }
                                Value::TimestampWithTimezone(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_timestamptz(&v))
                                }
                                Value::Interval(Some(v)) => {
                                    Self::write(data, len, ValueWrap::from_interval(&v))
                                }
                                Value::Uuid(Some(v)) => {
                                    let (upper, lower) = v.as_u64_pair();
                                    Self::write(
                                        data,
                                        len,
                                        duckdb_hugeint {
                                            lower,
                                            upper: (upper ^ (1 << 63)) as i64,
                                        },
                                    )
                                }
                                _ => {
                                    duckdb_vector_ensure_validity_writable(vector);
                                    duckdb_validity_set_row_invalid(
                                        duckdb_vector_get_validity(vector),
                                        len as _,
                                    );
                                }
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
