use crate::{
    DuckDBPrepared, DuckDBTransaction, cbox::CBox, date_to_duckdb_date, decimal_to_duckdb_decimal,
    driver::DuckDBDriver, i128_to_duckdb_hugeint, interval_to_duckdb_interval,
    offsetdatetime_to_duckdb_timestamp, primitive_date_time_to_duckdb_timestamp,
    tank_value_to_duckdb_logical_type, tank_value_to_duckdb_value, time_to_duckdb_time,
    u128_to_duckdb_uhugeint,
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
    AsEntity, AsQuery, Connection, Driver, Entity, Error, ErrorContext, Executor, Query, RawQuery,
    Result, RowsAffected, SqlCoreWriter, Value, as_c_string, describe_path, error_message_from_ptr,
    truncate_long,
};
use tokio::task::spawn_blocking;

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

    async fn do_prepare(&mut self, RawQuery { sql, .. }: RawQuery) -> Result<Query<DuckDBDriver>> {
        let connection = AtomicPtr::new(*self.connection);
        let prepared = spawn_blocking(move || unsafe {
            let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
            let mut prepared = CBox::new(ptr::null_mut(), |mut p| duckdb_destroy_prepare(&mut p));
            let sql = match CString::new(sql.as_bytes()) {
                Ok(sql) => sql,
                Err(e) => {
                    let error = Error::new(e)
                        .context("Could convert `String` to `CString`")
                        .context(make_context());
                    log::error!("{error:#}");
                    return Err(error);
                }
            };
            let rc = duckdb_prepare(
                connection.load(Ordering::Relaxed),
                sql.as_ptr(),
                &mut *prepared,
            );
            if rc != duckdb_state_DuckDBSuccess {
                let error = Error::msg(
                    error_message_from_ptr(&duckdb_prepare_error(*prepared)).to_string(),
                )
                .context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            Ok(prepared)
        })
        .await?;
        Ok(DuckDBPrepared::new(prepared?).into())
    }

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as tank_core::Driver>::Results<'s, Q::Output>>>
    + Send
    where
        Q: AsQuery<DuckDBDriver> + Send + 's,
    {
        async move { crate::streaming::Statements::new(self, query.as_query()) }
    }

    async fn append<It>(&mut self, rows: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let connection = AtomicPtr::new(*self.connection);
        let rows = rows
            .into_iter()
            .map(|e| e.as_entity().row_values())
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(Default::default());
        }
        let delimiter = self
            .driver()
            .sql_writer()
            .separator()
            .chars()
            .next()
            .unwrap();
        spawn_blocking(move || unsafe {
            let table_ref = E::<It>::table();
            let mut appender = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_appender_destroy(&mut p);
            });
            let connection = connection.load(Ordering::Relaxed);
            let rc = if let Some((catalog, schema)) = table_ref.schema.rsplit_once(delimiter) {
                duckdb_appender_create_ext(
                    connection,
                    as_c_string(catalog).as_ptr(),
                    as_c_string(schema).as_ptr(),
                    as_c_string((&table_ref.name) as &str).as_ptr(),
                    &mut *appender,
                )
            } else {
                duckdb_appender_create(
                    connection,
                    as_c_string(&table_ref.schema as &str).as_ptr(),
                    as_c_string(&table_ref.name as &str).as_ptr(),
                    &mut *appender,
                )
            };
            if rc != duckdb_state_DuckDBSuccess {
                return Err(Error::msg(
                    error_message_from_ptr(&duckdb_appender_error(*appender)).to_string(),
                )
                .context("While creating the `duckdb_appender` object"));
            }
            for column in E::<It>::columns() {
                let rc = duckdb_appender_add_column(*appender, as_c_string(column.name()).as_ptr());
                if rc != duckdb_state_DuckDBSuccess {
                    let error = Error::msg(
                        error_message_from_ptr(&duckdb_appender_error(*appender)).to_string(),
                    );
                    log::error!("{error:#}");
                    return Err(error);
                }
            }
            let rows_affected = rows.len() as u64;
            for row in rows {
                for value in row {
                    let rc = match value {
                        Value::Boolean(Some(v), ..) => duckdb_append_bool(*appender, v),
                        Value::Int8(Some(v), ..) => duckdb_append_int8(*appender, v),
                        Value::Int16(Some(v), ..) => duckdb_append_int16(*appender, v),
                        Value::Int32(Some(v), ..) => duckdb_append_int32(*appender, v),
                        Value::Int64(Some(v), ..) => duckdb_append_int64(*appender, v),
                        Value::Int128(Some(v), ..) => {
                            duckdb_append_hugeint(*appender, i128_to_duckdb_hugeint(*v))
                        }
                        Value::UInt8(Some(v), ..) => duckdb_append_uint8(*appender, v),
                        Value::UInt16(Some(v), ..) => duckdb_append_uint16(*appender, v),
                        Value::UInt32(Some(v), ..) => duckdb_append_uint32(*appender, v),
                        Value::UInt64(Some(v), ..) => duckdb_append_uint64(*appender, v),
                        Value::UInt128(Some(v), ..) => {
                            duckdb_append_uhugeint(*appender, u128_to_duckdb_uhugeint(*v))
                        }
                        Value::Float32(Some(v), ..) => duckdb_append_float(*appender, v),
                        Value::Float64(Some(v), ..) => duckdb_append_double(*appender, v),
                        Value::Decimal(Some(v), width, scale) => {
                            let value = CBox::new(
                                duckdb_create_decimal(decimal_to_duckdb_decimal(&v, width, scale)),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            duckdb_append_value(*appender, *value)
                        }
                        Value::Char(Some(v), ..) => {
                            duckdb_append_varchar(*appender, as_c_string(v.to_string()).as_ptr())
                        }
                        Value::Varchar(Some(v), ..) => duckdb_append_varchar(
                            *appender,
                            match v {
                                Cow::Borrowed(v) => as_c_string(v),
                                Cow::Owned(v) => as_c_string(v),
                            }
                            .as_ptr(),
                        ),
                        Value::Blob(Some(v), ..) => duckdb_append_blob(
                            *appender,
                            v.as_ptr() as *const c_void,
                            v.len() as u64,
                        ),
                        Value::Date(Some(v), ..) => {
                            duckdb_append_date(*appender, date_to_duckdb_date(&v))
                        }
                        Value::Time(Some(v), ..) => {
                            duckdb_append_time(*appender, time_to_duckdb_time(&v))
                        }
                        Value::Timestamp(Some(v), ..) => duckdb_append_timestamp(
                            *appender,
                            primitive_date_time_to_duckdb_timestamp(&v),
                        ),
                        Value::TimestampWithTimezone(Some(v), ..) => duckdb_append_timestamp(
                            *appender,
                            offsetdatetime_to_duckdb_timestamp(&v),
                        ),
                        Value::Interval(Some(ref v), ..) => {
                            duckdb_append_interval(*appender, interval_to_duckdb_interval(&v))
                        }
                        Value::Uuid(Some(ref v), ..) => duckdb_append_value(
                            *appender,
                            duckdb_create_uuid(u128_to_duckdb_uhugeint(v.as_u128())),
                        ),
                        Value::List(Some(ref v), ref ty) | Value::Array(Some(ref v), ref ty) => {
                            let logical_type = tank_value_to_duckdb_logical_type(&ty.element);
                            let values = v
                                .iter()
                                .map(|v| tank_value_to_duckdb_value(v))
                                .collect::<Vec<_>>();
                            let mut values = values.iter().map(|v| **v).collect::<Vec<_>>();
                            let is_array = matches!(value, Value::Array(..));
                            let value = CBox::new(
                                if is_array {
                                    duckdb_create_array_value(
                                        *logical_type,
                                        values.as_mut_ptr(),
                                        ty.len as u64,
                                    )
                                } else {
                                    duckdb_create_list_value(
                                        *logical_type,
                                        values.as_mut_ptr(),
                                        v.len() as u64,
                                    )
                                },
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create collection value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(*appender, *value)
                        }
                        Value::Map(Some(ref v), ..) => {
                            let logical_type = tank_value_to_duckdb_logical_type(&value);
                            let keys = v
                                .keys()
                                .map(|v| tank_value_to_duckdb_value(v))
                                .collect::<Vec<_>>();
                            let values = v
                                .values()
                                .map(|v| tank_value_to_duckdb_value(v))
                                .collect::<Vec<_>>();
                            let value = CBox::new(
                                duckdb_create_map_value(
                                    *logical_type,
                                    keys.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                                    values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                                    v.len() as u64,
                                ),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create map value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(*appender, *value)
                        }
                        Value::Struct(Some(ref v), ..) => {
                            let values = v
                                .iter()
                                .map(|v| tank_value_to_duckdb_value(&v.1))
                                .collect::<Vec<_>>();
                            let value = CBox::new(
                                duckdb_create_struct_value(
                                    *tank_value_to_duckdb_logical_type(&value),
                                    values.iter().map(|v| **v).collect::<Vec<_>>().as_mut_ptr(),
                                ),
                                |mut p| duckdb_destroy_value(&mut p),
                            );
                            if value.is_null() {
                                let error = anyhow!("Could not create struct value");
                                log::error!("{error:#}");
                                return Err(error);
                            }
                            duckdb_append_value(*appender, *value)
                        }
                        _ => duckdb_append_null(*appender),
                    };
                    if rc != duckdb_state_DuckDBSuccess {
                        let error = Error::msg(
                            error_message_from_ptr(&duckdb_appender_error(*appender)).to_string(),
                        );
                        log::error!("{error:#}");
                        return Err(error);
                    }
                }
                let rc = duckdb_appender_end_row(*appender);
                if rc != duckdb_state_DuckDBSuccess {
                    let error = Error::msg(
                        error_message_from_ptr(&duckdb_appender_error(*appender)).to_string(),
                    );
                    log::error!("{error:#}");
                    return Err(error);
                }
            }
            let rc = duckdb_appender_close(*appender);
            if rc != duckdb_state_DuckDBSuccess {
                let error = Error::msg(
                    error_message_from_ptr(&duckdb_appender_error(*appender)).to_string(),
                )
                .context("While closing the `duckdb_appender` object");
                log::error!("{error:#}");
                return Err(error);
            }
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            })
        })
        .await?
    }
}

impl Debug for DuckDBConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBConnection")
            .field("connection", &self.connection)
            .finish()
    }
}
