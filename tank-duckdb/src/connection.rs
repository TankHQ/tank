use crate::{
    DuckDBPrepared, DuckDBTransaction, cbox::CBox, date_to_duckdb_date, decimal_mantissa_at,
    decimal_to_duckdb_decimal,
    driver::DuckDBDriver, i128_to_duckdb_hugeint, interval_to_duckdb_interval,
    offsetdatetime_to_duckdb_timestamp, primitive_date_time_to_duckdb_timestamp,
    streaming::Statements, tank_value_to_duckdb_logical_type, tank_value_to_duckdb_value,
    time_to_duckdb_time, u128_to_duckdb_uhugeint, uuid_to_duckdb_hugeint,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char, c_void},
    fmt::{self, Debug, Formatter},
    ptr,
    str::FromStr,
    sync::{
        LazyLock,
        atomic::{AtomicPtr, Ordering},
    },
};
use tank_core::{
    AsEntity, AsQuery, ColumnDef, Connection, Driver, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, Result, Row, RowsAffected, SqlWriter, Value, as_c_string, describe_path,
    error_message_from_ptr,
    future::Either,
    stream::{self, Stream},
    truncate_long,
};

/// Wrapper around a DuckDB `duckdb_connection` pointer used by the DuckDB driver.
/// Provides helpers to execute queries and extract results into `tank_core` types.
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_connection>,
}

impl DuckDBConnection {
    pub(crate) fn database_cache() -> &'static AtomicPtr<_duckdb_instance_cache> {
        static DATABASE_CACHE: LazyLock<CBox<AtomicPtr<_duckdb_instance_cache>>> =
            LazyLock::new(|| {
                CBox::new(
                    AtomicPtr::new(unsafe { duckdb_create_instance_cache() }),
                    |ptr| unsafe {
                        duckdb_destroy_instance_cache(&mut ptr.load(Ordering::Relaxed))
                    },
                )
            });
        &**DATABASE_CACHE
    }

    /// Whether every column can be written straight into a data chunk vector.
    fn supports_chunk_append(columns: &[ColumnDef]) -> bool {
        !columns.is_empty()
            && columns.iter().all(|column| {
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
                )
            })
    }

    /// Appends the rows one cell at a time; needed when a column is not a plain scalar.
    unsafe fn append_cells<It>(appender: duckdb_appender, rows: It) -> Result<u64>
    where
        It: IntoIterator,
        It::Item: AsEntity,
    {
        unsafe {
            let mut rows_affected = 0;
            for entity in rows {
                for value in entity.as_entity().row_values() {
                    let rc = match value {
                        Value::Boolean(Some(v)) => duckdb_append_bool(appender, v),
                        Value::Int8(Some(v)) => duckdb_append_int8(appender, v),
                        Value::Int16(Some(v)) => duckdb_append_int16(appender, v),
                        Value::Int32(Some(v)) => duckdb_append_int32(appender, v),
                        Value::Int64(Some(v)) => duckdb_append_int64(appender, v),
                        Value::Int128(Some(v)) => {
                            duckdb_append_hugeint(appender, i128_to_duckdb_hugeint(*v))
                        }
                        Value::UInt8(Some(v)) => duckdb_append_uint8(appender, v),
                        Value::UInt16(Some(v)) => duckdb_append_uint16(appender, v),
                        Value::UInt32(Some(v)) => duckdb_append_uint32(appender, v),
                        Value::UInt64(Some(v)) => duckdb_append_uint64(appender, v),
                        Value::UInt128(Some(v)) => {
                            duckdb_append_uhugeint(appender, u128_to_duckdb_uhugeint(*v))
                        }
                        Value::Float32(Some(v)) => duckdb_append_float(appender, v),
                        Value::Float64(Some(v)) => duckdb_append_double(appender, v),
                        Value::Decimal(Some(v), width, scale) => {
                            let value = CBox::new(
                                duckdb_create_decimal(decimal_to_duckdb_decimal(&v, width, scale)),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            duckdb_append_value(appender, *value)
                        }
                        Value::Char(Some(v)) => {
                            let mut buf = [0u8; 4];
                            let v = v.encode_utf8(&mut buf);
                            duckdb_append_varchar_length(appender, v.as_ptr() as *const c_char, v.len() as _)
                        }
                        Value::Varchar(Some(v)) => duckdb_append_varchar_length(
                            appender,
                            v.as_ptr() as *const c_char,
                            v.len() as _,
                        ),
                        Value::Blob(Some(v)) => {
                            duckdb_append_blob(appender, v.as_ptr() as *const c_void, v.len() as _)
                        }
                        Value::Date(Some(v)) => duckdb_append_date(appender, date_to_duckdb_date(&v)),
                        Value::Time(Some(v)) => duckdb_append_time(appender, time_to_duckdb_time(&v)),
                        Value::Timestamp(Some(v)) => duckdb_append_timestamp(
                            appender,
                            primitive_date_time_to_duckdb_timestamp(&v),
                        ),
                        Value::TimestampWithTimezone(Some(v)) => duckdb_append_timestamp(
                            appender,
                            offsetdatetime_to_duckdb_timestamp(&v),
                        ),
                        Value::Interval(Some(ref v)) => {
                            duckdb_append_interval(appender, interval_to_duckdb_interval(v))
                        }
                        Value::Uuid(Some(ref v)) => {
                            let value = CBox::new(
                                duckdb_create_uuid(u128_to_duckdb_uhugeint(v.as_u128())),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            duckdb_append_value(appender, *value)
                        }
                        Value::List(Some(ref v), ref ty) | Value::Array(Some(ref v), ref ty) => {
                            let logical_type = tank_value_to_duckdb_logical_type(&ty.element);
                            let values = v
                                .iter()
                                .map(tank_value_to_duckdb_value)
                                .collect::<Vec<_>>();
                            let mut values = values.iter().map(|v| **v).collect::<Vec<_>>();
                            let is_array = matches!(value, Value::Array(..));
                            let value = CBox::new(
                                if is_array {
                                    duckdb_create_array_value(
                                        *logical_type,
                                        values.as_mut_ptr(),
                                        ty.len as _,
                                    )
                                } else {
                                    duckdb_create_list_value(
                                        *logical_type,
                                        values.as_mut_ptr(),
                                        v.len() as _,
                                    )
                                },
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create a collection value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(appender, *value)
                        }
                        Value::Map(Some(ref v), ..) => {
                            let logical_type = tank_value_to_duckdb_logical_type(&value);
                            let keys = v.keys().map(tank_value_to_duckdb_value).collect::<Vec<_>>();
                            let values = v
                                .values()
                                .map(tank_value_to_duckdb_value)
                                .collect::<Vec<_>>();
                            let mut raw_keys = keys.iter().map(|v| **v).collect::<Vec<_>>();
                            let mut raw_values = values.iter().map(|v| **v).collect::<Vec<_>>();
                            let value = CBox::new(
                                duckdb_create_map_value(
                                    *logical_type,
                                    raw_keys.as_mut_ptr(),
                                    raw_values.as_mut_ptr(),
                                    v.len() as _,
                                ),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create a map value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(appender, *value)
                        }
                        Value::Struct(Some(ref v), ..) => {
                            let values = v
                                .iter()
                                .map(|v| tank_value_to_duckdb_value(&v.1))
                                .collect::<Vec<_>>();
                            let mut raw_values = values.iter().map(|v| **v).collect::<Vec<_>>();
                            let value = CBox::new(
                                duckdb_create_struct_value(
                                    *tank_value_to_duckdb_logical_type(&value),
                                    raw_values.as_mut_ptr(),
                                ),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create a struct value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(appender, *value)
                        }
                        _ => duckdb_append_null(appender),
                    };
                    Self::check_appender(appender, rc)?;
                }
                Self::check_appender(appender, duckdb_appender_end_row(appender))?;
                rows_affected += 1;
            }
            Ok(rows_affected)
        }
    }

    /// Writes one cell into a chunk vector, which is typed after the value's own column.
    #[inline(always)]
    unsafe fn write_chunk_cell(
        vector: duckdb_vector,
        data: *mut c_void,
        row: usize,
        width: u8,
        scale: u8,
        val: &Value<'_>,
    ) -> Result<()> {
        unsafe {
            if val.is_null() {
                duckdb_vector_ensure_validity_writable(vector);
                duckdb_validity_set_row_invalid(duckdb_vector_get_validity(vector), row as _);
                return Ok(());
            }
            let put_string = |src: &[u8]| {
                duckdb_vector_assign_string_element_len(
                    vector,
                    row as _,
                    src.as_ptr() as *const c_char,
                    src.len() as _,
                )
            };
            match val {
                Value::Boolean(Some(v)) => *(data as *mut bool).add(row) = *v,
                Value::Int8(Some(v)) => *(data as *mut i8).add(row) = *v,
                Value::Int16(Some(v)) => *(data as *mut i16).add(row) = *v,
                Value::Int32(Some(v)) => *(data as *mut i32).add(row) = *v,
                Value::Int64(Some(v)) => *(data as *mut i64).add(row) = *v,
                Value::Int128(Some(v)) => {
                    *(data as *mut duckdb_hugeint).add(row) = i128_to_duckdb_hugeint(**v)
                }
                Value::UInt8(Some(v)) => *(data as *mut u8).add(row) = *v,
                Value::UInt16(Some(v)) => *(data as *mut u16).add(row) = *v,
                Value::UInt32(Some(v)) => *(data as *mut u32).add(row) = *v,
                Value::UInt64(Some(v)) => *(data as *mut u64).add(row) = *v,
                Value::UInt128(Some(v)) => {
                    *(data as *mut duckdb_uhugeint).add(row) = u128_to_duckdb_uhugeint(**v)
                }
                Value::Float32(Some(v)) => *(data as *mut f32).add(row) = *v,
                Value::Float64(Some(v)) => *(data as *mut f64).add(row) = *v,
                Value::Decimal(Some(v), ..) => {
                    let mantissa = decimal_mantissa_at(v, scale);
                    if width <= 4 {
                        *(data as *mut i16).add(row) = mantissa as i16;
                    } else if width <= 9 {
                        *(data as *mut i32).add(row) = mantissa as i32;
                    } else if width <= 18 {
                        *(data as *mut i64).add(row) = mantissa as i64;
                    } else {
                        *(data as *mut duckdb_hugeint).add(row) = i128_to_duckdb_hugeint(mantissa);
                    }
                }
                Value::Char(Some(v)) => {
                    let mut buf = [0u8; 4];
                    put_string(v.encode_utf8(&mut buf).as_bytes())
                }
                Value::Varchar(Some(v)) => put_string(v.as_bytes()),
                Value::Blob(Some(v)) => put_string(v.as_ref()),
                Value::Date(Some(v)) => *(data as *mut i32).add(row) = date_to_duckdb_date(v).days,
                Value::Time(Some(v)) => *(data as *mut i64).add(row) = time_to_duckdb_time(v).micros,
                Value::Timestamp(Some(v)) => {
                    *(data as *mut i64).add(row) = primitive_date_time_to_duckdb_timestamp(v).micros
                }
                Value::TimestampWithTimezone(Some(v)) => {
                    *(data as *mut i64).add(row) = offsetdatetime_to_duckdb_timestamp(v).micros
                }
                Value::Interval(Some(v)) => {
                    *(data as *mut duckdb_interval).add(row) = interval_to_duckdb_interval(v)
                }
                Value::Uuid(Some(v)) => {
                    *(data as *mut duckdb_hugeint).add(row) = uuid_to_duckdb_hugeint(v)
                }
                _ => return Err(anyhow!("Unsupported value for a vectorised append: {val:?}")),
            }
            Ok(())
        }
    }

    /// Appends the rows through a single data chunk, one vectorised write per column and batch.
    unsafe fn append_chunked<It>(
        appender: duckdb_appender,
        columns: &[ColumnDef],
        rows: It,
    ) -> Result<u64>
    where
        It: IntoIterator,
        It::Item: AsEntity,
    {
        unsafe {
            let types = columns
                .iter()
                .map(|column| tank_value_to_duckdb_logical_type(&column.value))
                .collect::<Box<[_]>>();
            let mut raw_types = types.iter().map(|t| **t).collect::<Box<[_]>>();
            let chunk = CBox::new(
                duckdb_create_data_chunk(raw_types.as_mut_ptr(), raw_types.len() as _),
                |mut chunk| duckdb_destroy_data_chunk(&mut chunk),
            );
            if chunk.is_null() {
                return Err(anyhow!("Could not create a DuckDB data chunk"));
            }
            // A DECIMAL vector stores its digits scaled to the declared scale.
            let decimals = types
                .iter()
                .map(|t| {
                    if duckdb_get_type_id(**t) == DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
                        (duckdb_decimal_width(**t), duckdb_decimal_scale(**t))
                    } else {
                        (0, 0)
                    }
                })
                .collect::<Box<[(u8, u8)]>>();
            let batch_size = duckdb_vector_size() as usize;
            let mut rows = rows.into_iter();
            let mut rows_affected = 0;
            let mut cells = Vec::with_capacity(raw_types.len());
            loop {
                duckdb_data_chunk_reset(*chunk);
                cells.clear();
                for i in 0..raw_types.len() {
                    let vector = duckdb_data_chunk_get_vector(*chunk, i as _);
                    cells.push((vector, duckdb_vector_get_data(vector)));
                }
                let mut len = 0;
                while len < batch_size {
                    let Some(entity) = rows.next() else {
                        break;
                    };
                    let row = entity.as_entity().row_values();
                    for ((value, &(vector, data)), &(width, scale)) in
                        row.iter().zip(&cells).zip(&decimals)
                    {
                        Self::write_chunk_cell(vector, data, len, width, scale, value)?;
                    }
                    len += 1;
                }
                if len == 0 {
                    break;
                }
                duckdb_data_chunk_set_size(*chunk, len as _);
                Self::check_appender(appender, duckdb_append_data_chunk(appender, *chunk))?;
                rows_affected += len as u64;
            }
            Ok(rows_affected)
        }
    }
    #[inline(always)]
    unsafe fn check_appender(appender: duckdb_appender, rc: duckdb_state) -> Result<()> {
        if rc == duckdb_state_DuckDBSuccess {
            return Ok(());
        }
        let raw = unsafe { duckdb_appender_error(appender) };
        let error = Error::msg(error_message_from_ptr(&raw).to_string());
        log::error!("{error:#}");
        Err(error)
    }
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to DuckDB {}",
                describe_path::<DuckDBDriver>(&url)
            )
        };
        let mut config: CBox<duckdb_config> = CBox::new(ptr::null_mut(), |mut p| unsafe {
            duckdb_destroy_config(&mut p)
        });
        unsafe {
            let rc = duckdb_create_config(&mut *config);
            if rc != duckdb_state_DuckDBSuccess {
                let error =
                    anyhow!("Cannot allocate the duckdb_config object").context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
        };
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
                                    let error = anyhow!("Unknown value {value:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`");
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
        let mut database: duckdb_database = ptr::null_mut();
        let mut connection;
        let mut error: CBox<*mut c_char> = CBox::new(ptr::null_mut(), |p| unsafe {
            duckdb_free(p as *mut c_void)
        });
        let db_cache = Self::database_cache().load(Ordering::Relaxed);
        unsafe {
            let rc = duckdb_get_or_create_from_cache(
                db_cache,
                path.as_ptr(),
                &mut database,
                *config,
                &mut *error,
            );
            if rc != duckdb_state_DuckDBSuccess {
                let error = CStr::from_ptr(*error)
                    .to_str()
                    .context("While reading the error from `duckdb_get_or_create_from_cache`")?
                    .to_owned();
                return Err(Error::msg(error));
            };
            connection = CBox::new(ptr::null_mut(), |mut p| duckdb_disconnect(&mut p));
            let rc = duckdb_connect(database, &mut *connection);
            if rc != duckdb_state_DuckDBSuccess {
                let error = anyhow!("Failed to connect to database url `{url}`");
                log::error!("{error:#}");
                return Err(error);
            };
        };
        Ok(DuckDBConnection { connection })
    }

    fn begin(&mut self) -> impl Future<Output = Result<DuckDBTransaction<'_>>> + Send {
        DuckDBTransaction::new(self)
    }
}

impl Executor for DuckDBConnection {
    type Driver = DuckDBDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<DuckDBDriver>> {
        let prepared = (|| unsafe {
            let c_sql =
                CString::new(sql.as_bytes()).context("Could not convert `String` to `CString`")?;
            let mut prepared = CBox::new(ptr::null_mut(), |mut p| duckdb_destroy_prepare(&mut p));
            if duckdb_prepare(*self.connection, c_sql.as_ptr(), &mut *prepared)
                != duckdb_state_DuckDBSuccess
            {
                let raw = duckdb_prepare_error(*prepared);
                return Err(Error::msg(error_message_from_ptr(&raw).to_string()));
            }
            Ok(prepared)
        })()
        .with_context(|| format!("While preparing the query:\n{}", truncate_long!(sql)))
        .inspect_err(|error| log::error!("{error:#}"))?;
        Ok(DuckDBPrepared::new(prepared).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<DuckDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        match Statements::new(self, query.as_query()) {
            Ok(statements) => Either::Left(stream::iter(statements)),
            Err(error) => Either::Right(stream::once(std::future::ready(Err(error)))),
        }
    }

    async fn append<It>(&mut self, rows: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        unsafe {
            let connection = *self.connection;
            let table = E::<It>::table();
            let delimiter = self
                .driver()
                .sql_writer()
                .separator()
                .chars()
                .next()
                .unwrap_or('.');
            let mut appender = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_appender_destroy(&mut p);
            });
            let rc = match table.schema.rsplit_once(delimiter) {
                Some((catalog, schema)) => duckdb_appender_create_ext(
                    connection,
                    as_c_string(catalog).as_ptr(),
                    as_c_string(schema).as_ptr(),
                    as_c_string((&table.name) as &str).as_ptr(),
                    &mut *appender,
                ),
                None => duckdb_appender_create(
                    connection,
                    as_c_string(&table.schema as &str).as_ptr(),
                    as_c_string(&table.name as &str).as_ptr(),
                    &mut *appender,
                ),
            };
            Self::check_appender(*appender, rc)
                .context("While creating the `duckdb_appender` object")?;
            for column in E::<It>::columns() {
                Self::check_appender(
                    *appender,
                    duckdb_appender_add_column(*appender, as_c_string(column.name()).as_ptr()),
                )?;
            }
            let columns = E::<It>::columns();
            let rows_affected = if Self::supports_chunk_append(columns) {
                Self::append_chunked(*appender, columns, rows)?
            } else {
                Self::append_cells(*appender, rows)?
            };
            Self::check_appender(*appender, duckdb_appender_close(*appender))
                .context("While closing the `duckdb_appender` object")?;
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            })
        }
    }
}

impl Debug for DuckDBConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBConnection")
            .field("connection", &self.connection)
            .finish()
    }
}
