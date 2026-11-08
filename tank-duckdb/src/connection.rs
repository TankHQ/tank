use crate::{
    DuckDBPrepared, DuckDBTransaction, ValueWrap, cbox::CBox, check_rc, date_to_duckdb_date,
    decimal_to_duckdb_decimal, driver::DuckDBDriver, i128_to_duckdb_hugeint,
    interval_to_duckdb_interval, offsetdatetime_to_duckdb_timestamp,
    primitive_date_time_to_duckdb_timestamp, streaming::Statements, time_to_duckdb_time,
    u128_to_duckdb_uhugeint, uuid_to_duckdb_hugeint,
};
use anyhow::anyhow;
use libduckdb_sys::v2::*;
use std::{
    borrow::Cow,
    cmp::max,
    collections::HashMap,
    ffi::{CString, c_void},
    fmt::{self, Debug, Formatter},
    mem, ptr,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tank_core::{
    AsEntity, AsQuery, Connection, Context, Driver, DynQuery, Entity, ErrorContext, Executor,
    Fragment, Query, QueryResult, Result, Row, RowLabels, RowsAffected, SqlWriter, Value,
    future::Either,
    stream::{self, Stream},
    truncate_long,
};

/// Wrapper around a DuckDB `duckdb_v2_connection_handle`.
/// Provides helpers to execute queries and extract results into `tank_core` types.
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_v2_connection_handle>,
    pub(crate) factory: duckdb_v2_factory_handle,
    pub(crate) labels: RowLabels,
    pub(crate) _instance: Arc<CBox<duckdb_v2_instance_handle>>,
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        static ENVIRONMENT: OnceLock<(
            CBox<duckdb_v2_environment_handle>,
            Mutex<HashMap<String, Weak<CBox<duckdb_v2_instance_handle>>>>,
        )> = OnceLock::new();
        let url = Self::sanitize_url(driver, url)?;
        let mut path = format!(
            "{}{}",
            url.host_str()
                .map_or(Default::default(), |host| format!("{host}/")),
            url.path()
        );
        if path.is_empty() {
            path = ":memory:".into();
        }
        let mut options = Vec::new();
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "mode" => {
                    if v == "memory" {
                        path = ":memory:".to_owned();
                        continue;
                    }
                    let access_mode = match &*v {
                        "ro" => "READ_ONLY",
                        "rw" | "rwc" => "READ_WRITE",
                        _ => {
                            let error = anyhow!(
                                "Unknown value {v:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`"
                            );
                            log::warn!("{error:#}");
                            return Err(error);
                        }
                    };
                    options.push((Cow::Borrowed("access_mode"), Cow::Borrowed(access_mode)));
                }
                _ => options.push((k, v)),
            }
        }
        (|| -> Result<Self> {
            unsafe {
                let (environment, instances) = if let Some(env) = ENVIRONMENT.get() {
                    env
                } else {
                    let mut env = CBox::new(ptr::null_mut(), |mut p| {
                        duckdb_v2_environment_destroy(&mut p);
                    });
                    check_rc(|err| duckdb_v2_environment_create(&mut *env, err))?;
                    let _ = ENVIRONMENT.set((env, Default::default()));
                    ENVIRONMENT
                        .get()
                        .context("While accessing the DuckDB environment")?
                };
                let mut cache = instances
                    .lock()
                    .ok()
                    .context("DuckDB instance cache mutex was poisoned")?;
                let is_memory = path == ":memory:";
                let instance = match (!is_memory)
                    .then(|| cache.get(&path).and_then(Weak::upgrade))
                    .flatten()
                {
                    Some(existing) => existing,
                    None => {
                        let names: Vec<duckdb_v2_str> =
                            options.iter().map(|(k, _)| k.as_ref().into()).collect();
                        let values: Vec<duckdb_v2_str> =
                            options.iter().map(|(_, v)| v.as_ref().into()).collect();
                        let mut instance = CBox::new(ptr::null_mut(), |mut p| {
                            duckdb_v2_instance_destroy(&mut p);
                        });
                        check_rc(|err| {
                            duckdb_v2_instance_create_with_options(
                                **environment,
                                if names.is_empty() {
                                    ptr::null()
                                } else {
                                    names.as_ptr()
                                },
                                if values.is_empty() {
                                    ptr::null()
                                } else {
                                    values.as_ptr()
                                },
                                options.len() as _,
                                &mut *instance,
                                err,
                            )
                        })?;
                        let path_str = path.as_str().into();
                        check_rc(|err| {
                            duckdb_v2_instance_attach(
                                *instance,
                                ptr::null(),
                                &path_str,
                                ptr::null_mut(),
                                true,
                                err,
                            )
                        })?;
                        let instance = Arc::new(instance);
                        if !is_memory {
                            cache.insert(path, Arc::downgrade(&instance));
                        }
                        instance
                    }
                };
                drop(cache);
                let mut connection = CBox::new(ptr::null_mut(), |mut p| {
                    duckdb_v2_connection_destroy(&mut p);
                });
                check_rc(|err| duckdb_v2_connection_create(**instance, &mut *connection, err))?;
                let mut factory = ptr::null_mut();
                check_rc(|err| duckdb_v2_connection_get_factory(*connection, &mut factory, err))?;
                if !options
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("max_streaming_buffer_size"))
                {
                    let mut config = ptr::null_mut();
                    check_rc(|err| duckdb_v2_connection_get_config(*connection, &mut config, err))?;
                    check_rc(|err| {
                        duckdb_v2_config_set_option_text(
                            config,
                            &"max_streaming_buffer_size".into(),
                            &"256KiB".into(),
                            DUCKDB_V2_SETTING_SCOPE::DUCKDB_V2_SETTING_SCOPE_SESSION,
                            err,
                        )
                    })?;
                }
                Ok(DuckDBConnection {
                    connection,
                    factory,
                    labels: RowLabels::default(),
                    _instance: instance,
                })
            }
        })()
        .context("While trying to connect to DuckDB")
    }

    fn begin(&mut self) -> impl Future<Output = Result<DuckDBTransaction<'_>>> + Send {
        DuckDBTransaction::new(self)
    }
}

impl Executor for DuckDBConnection {
    type Driver = DuckDBDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<DuckDBDriver>> {
        let (prepared, param_count) = (|| unsafe {
            let c_sql =
                CString::new(sql.as_bytes()).context("Could not convert `String` to `CString`")?;
            let mut iterator = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_statement_iterator_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_parse_sql(*self.connection, c_sql.as_ptr(), &mut *iterator, err)
            })?;
            let mut parsed = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_sql_statement_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_statement_iterator_next(*iterator, &mut *parsed, err))?;
            if parsed.is_null() {
                return Err(anyhow!("No statement to prepare"));
            }
            let mut extra = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_sql_statement_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_statement_iterator_next(*iterator, &mut *extra, err))?;
            if !extra.is_null() {
                return Err(anyhow!("Cannot prepare multiple statements at once"));
            }
            let mut param_count = 0;
            check_rc(|err| {
                duckdb_v2_sql_statement_get_parameter_count(*parsed, &mut param_count, err)
            })?;
            let mut prepared = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_prepared_statement_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_prepared_statement_create(
                    *self.connection,
                    *parsed,
                    false,
                    &mut *prepared,
                    err,
                )
            })?;
            Ok((prepared, param_count))
        })()
        .with_context(|| format!("While preparing the query:\n{}", truncate_long!(sql)))
        .inspect_err(|error| log::error!("{error:#}"))?;
        Ok(Query::Prepared(DuckDBPrepared::new(
            prepared,
            param_count,
            sql,
        )))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<DuckDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        match Statements::new(self, query.as_query()) {
            Ok(statements) => Either::Left(statements),
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
        let mut rows = rows.into_iter();
        let Some(first) = rows.next() else {
            return Ok(Default::default());
        };
        unsafe {
            let factory = self.factory;
            let col_types = first
                .as_entity()
                .row_values()
                .iter()
                .map(|v| ValueWrap::from(v).to_logical_type(factory))
                .collect::<Result<Box<[_]>>>()?;
            let raw_types = col_types.iter().map(|t| **t).collect::<Box<[_]>>();
            let mut collection = CBox::new(ptr::null_mut(), |mut c| {
                duckdb_v2_column_data_collection_destroy(&mut c);
            });
            check_rc(|err| {
                duckdb_v2_column_data_collection_create(
                    factory,
                    raw_types.as_ptr(),
                    raw_types.len() as _,
                    &mut *collection,
                    err,
                )
            })?;
            let mut append_state = CBox::new(ptr::null_mut(), |mut s| {
                duckdb_v2_column_data_collection_append_state_destroy(&mut s);
            });
            check_rc(|err| {
                duckdb_v2_column_data_collection_append_state_create(
                    *collection,
                    &mut *append_state,
                    err,
                )
            })?;
            let mut chunk = CBox::new(ptr::null_mut(), |mut c| {
                duckdb_v2_data_chunk_destroy(&mut c);
            });
            check_rc(|err| {
                duckdb_v2_data_chunk_create(
                    factory,
                    raw_types.as_ptr(),
                    raw_types.len() as _,
                    &mut *chunk,
                    err,
                )
            })?;
            let mut capacity = 0;
            check_rc(|err| duckdb_v2_data_chunk_get_capacity(*chunk, &mut capacity, err))?;
            let batch_cap = max(capacity as usize, 1);
            let mut vectors = (0..raw_types.len())
                .map(|i| {
                    let mut vector = ptr::null_mut();
                    check_rc(|err| {
                        duckdb_v2_data_chunk_get_vector(*chunk, i as _, &mut vector, err)
                    })?;
                    Ok((vector, ptr::null_mut::<c_void>()))
                })
                .collect::<Result<Box<[_]>>>()?;
            let mut first = Some(first);
            while let Some(head) = first.take().or_else(|| rows.next()) {
                check_rc(|err| duckdb_v2_data_chunk_clear(*chunk, err))?;
                for (vector, data) in &mut vectors {
                    check_rc(|err| duckdb_v2_vector_set_size(*vector, batch_cap as _, err))?;
                    check_rc(|err| duckdb_v2_vector_get_data_mutable(*vector, data, err))?;
                }
                let mut batch_len = 0;
                for item in std::iter::once(head).chain(rows.by_ref().take(batch_cap - 1)) {
                    let row = item.as_entity().row_values();
                    for (&(vector, data), val) in vectors.iter().zip(&row) {
                        if val.is_null() {
                            check_rc(|err| duckdb_v2_vector_set_null(vector, batch_len as _, err))?;
                            continue;
                        }
                        match val {
                            Value::Boolean(Some(v)) => *(data as *mut bool).add(batch_len) = *v,
                            Value::Int8(Some(v)) => *(data as *mut i8).add(batch_len) = *v,
                            Value::Int16(Some(v)) => *(data as *mut i16).add(batch_len) = *v,
                            Value::Int32(Some(v)) => *(data as *mut i32).add(batch_len) = *v,
                            Value::Int64(Some(v)) => *(data as *mut i64).add(batch_len) = *v,
                            Value::Int128(Some(v)) => {
                                *(data as *mut duckdb_v2_hugeint_t).add(batch_len) =
                                    i128_to_duckdb_hugeint(**v)
                            }
                            Value::UInt8(Some(v)) => *(data as *mut u8).add(batch_len) = *v,
                            Value::UInt16(Some(v)) => *(data as *mut u16).add(batch_len) = *v,
                            Value::UInt32(Some(v)) => *(data as *mut u32).add(batch_len) = *v,
                            Value::UInt64(Some(v)) => *(data as *mut u64).add(batch_len) = *v,
                            Value::UInt128(Some(v)) => {
                                *(data as *mut duckdb_v2_uhugeint_t).add(batch_len) =
                                    u128_to_duckdb_uhugeint(**v)
                            }
                            Value::Float32(Some(v)) => *(data as *mut f32).add(batch_len) = *v,
                            Value::Float64(Some(v)) => *(data as *mut f64).add(batch_len) = *v,
                            Value::Decimal(Some(v), width, scale) if *width > 0 => {
                                let (raw, width, _) = decimal_to_duckdb_decimal(v, *width, *scale);
                                if width <= 4 {
                                    *(data as *mut i16).add(batch_len) = raw.lower as i16;
                                } else if width <= 9 {
                                    *(data as *mut i32).add(batch_len) = raw.lower as i32;
                                } else if width <= 18 {
                                    *(data as *mut i64).add(batch_len) = raw.lower as i64;
                                } else {
                                    *(data as *mut duckdb_v2_hugeint_t).add(batch_len) = raw;
                                }
                            }
                            Value::Date(Some(v)) => {
                                *(data as *mut i32).add(batch_len) = date_to_duckdb_date(v)
                            }
                            Value::Time(Some(v)) => {
                                *(data as *mut i64).add(batch_len) = time_to_duckdb_time(v)
                            }
                            Value::Timestamp(Some(v)) => {
                                *(data as *mut i64).add(batch_len) =
                                    primitive_date_time_to_duckdb_timestamp(v)
                            }
                            Value::TimestampWithTimezone(Some(v)) => {
                                *(data as *mut i64).add(batch_len) =
                                    offsetdatetime_to_duckdb_timestamp(v)
                            }
                            Value::Interval(Some(v)) => {
                                *(data as *mut duckdb_v2_interval_t).add(batch_len) =
                                    interval_to_duckdb_interval(v)
                            }
                            Value::Uuid(Some(v)) => {
                                *(data as *mut duckdb_v2_hugeint_t).add(batch_len) =
                                    uuid_to_duckdb_hugeint(v)
                            }
                            Value::Char(Some(..))
                            | Value::Varchar(Some(..))
                            | Value::Blob(Some(..)) => {
                                let mut char_buf = [0u8; 4];
                                let src = match val {
                                    Value::Char(Some(v)) => v.encode_utf8(&mut char_buf).as_bytes(),
                                    Value::Varchar(Some(v)) => v.as_bytes(),
                                    Value::Blob(Some(v)) => v.as_ref(),
                                    _ => unreachable!(),
                                };
                                let len = src.len();
                                let mut bytes: duckdb_v2_bytes = mem::zeroed();
                                if len <= DUCKDB_V2_BYTES_INLINE_LENGTH as _ {
                                    bytes.value.inlined.length = len as _;
                                    ptr::copy_nonoverlapping(
                                        src.as_ptr(),
                                        bytes.value.inlined.inlined.as_mut_ptr() as _,
                                        len,
                                    );
                                } else {
                                    let mut arena = ptr::null_mut();
                                    let mut buf = ptr::null_mut();
                                    check_rc(|err| {
                                        duckdb_v2_vector_get_arena(vector, &mut arena, err)
                                    })?;
                                    check_rc(|err| {
                                        duckdb_v2_arena_allocate(arena, len as _, &mut buf, err)
                                    })?;
                                    bytes.value.pointer.length = len as _;
                                    ptr::copy_nonoverlapping(src.as_ptr(), buf, len);
                                    ptr::copy_nonoverlapping(
                                        src.as_ptr(),
                                        bytes.value.pointer.prefix.as_mut_ptr() as _,
                                        4,
                                    );
                                    bytes.value.pointer.ptr = buf as _;
                                }
                                *(data as *mut duckdb_v2_bytes).add(batch_len) = bytes;
                            }
                            _ => {
                                let d_val = ValueWrap::from(val).to_duckdb_value(factory)?;
                                check_rc(|err| {
                                    duckdb_v2_vector_set_value(vector, batch_len as _, *d_val, err)
                                })?;
                            }
                        }
                    }
                    batch_len += 1;
                }
                if batch_len < batch_cap {
                    for (vector, ..) in &vectors {
                        check_rc(|err| duckdb_v2_vector_set_size(*vector, batch_len as _, err))?;
                    }
                }
                check_rc(|err| {
                    duckdb_v2_column_data_collection_append(
                        *collection,
                        *append_state,
                        *chunk,
                        err,
                    )
                })?;
            }
            let writer = self.driver().sql_writer();
            let mut context = Context::new(Fragment::SqlInsertInto, false);
            let mut query = DynQuery::default();
            query.push_str("INSERT INTO ");
            writer.write_table_ref(&mut context, &mut query, E::<It>::table());
            query.push_str(" SELECT * FROM __tank_append");
            let c_sql = CString::new(query.as_str().as_bytes())
                .context("While converting append query to CString")?;
            let mut iterator = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_statement_iterator_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_parse_sql(*self.connection, c_sql.as_ptr(), &mut *iterator, err)
            })?;
            let mut stmt = CBox::new(ptr::null_mut(), |mut s| {
                duckdb_v2_sql_statement_destroy(&mut s);
            });
            check_rc(|err| duckdb_v2_statement_iterator_next(*iterator, &mut *stmt, err))?;
            check_rc(|err| {
                duckdb_v2_statement_add_collection(
                    *stmt,
                    &"__tank_append".into(),
                    *collection,
                    ptr::null(),
                    0,
                    err,
                )
            })?;
            let mut result = CBox::new(ptr::null_mut(), |mut r| {
                duckdb_v2_result_destroy(&mut r);
            });
            check_rc(|err| {
                duckdb_v2_statement_execute(
                    *self.connection,
                    *stmt,
                    ptr::null(),
                    ptr::null(),
                    0,
                    &mut *result,
                    err,
                )
            })?;
            let mut rows_affected = 0;
            check_rc(|err| duckdb_v2_result_drain(*result, &mut rows_affected, err))?;
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            })
        }
    }
}

unsafe impl Send for DuckDBConnection {}
unsafe impl Sync for DuckDBConnection {}

impl Debug for DuckDBConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBConnection")
            .field("connection", &self.connection)
            .finish()
    }
}
