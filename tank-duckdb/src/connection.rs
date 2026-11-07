use crate::{
    DuckDBPrepared, DuckDBTransaction, ValueWrap, cbox::CBox, check_rc, date_to_duckdb_date,
    driver::DuckDBDriver, i128_to_duckdb_hugeint, interval_to_duckdb_interval,
    offsetdatetime_to_duckdb_timestamp, primitive_date_time_to_duckdb_timestamp,
    streaming::Statements, time_to_duckdb_time, u128_to_duckdb_uhugeint, uuid_to_duckdb_hugeint,
};
use anyhow::anyhow;
use libduckdb_sys::v2::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::CString,
    fmt::{self, Debug, Formatter},
    future::poll_fn,
    mem, ptr,
    sync::{Arc, LazyLock, Mutex, Weak},
};
use tank_core::{
    AsEntity, AsQuery, Connection, Context, Driver, DynQuery, Entity, Error, ErrorContext,
    Executor, Fragment, Query, QueryResult, Result, Row, RowsAffected, SqlWriter, Value,
    describe_path,
    future::Either,
    separated_by,
    stream::{self, Stream},
    truncate_long,
};

#[derive(Debug)]
pub(crate) struct DuckDBInstance {
    pub(crate) handle: CBox<duckdb_v2_instance_handle>,
}

struct DuckDBEnv {
    env: CBox<duckdb_v2_environment_handle>,
    instances: Mutex<HashMap<String, Weak<DuckDBInstance>>>,
}

/// Wrapper around a DuckDB `duckdb_v2_connection_handle`.
/// Provides helpers to execute queries and extract results into `tank_core` types.
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_v2_connection_handle>,
    pub(crate) factory: duckdb_v2_factory_handle,
    pub(crate) _instance: Arc<DuckDBInstance>,
}

impl DuckDBConnection {
    fn env() -> &'static DuckDBEnv {
        static ENV: LazyLock<DuckDBEnv> = LazyLock::new(|| unsafe {
            let mut env = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_environment_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_environment_create(&mut *env, err))
                .expect("Failed to initialize DuckDB v2 environment");
            DuckDBEnv {
                env,
                instances: Mutex::new(HashMap::new()),
            }
        });
        &ENV
    }

    fn create_instance(path: &str, options: &[(String, String)]) -> Result<Arc<DuckDBInstance>> {
        let env = Self::env();
        let names = options
            .iter()
            .map(|(k, _)| duckdb_v2_str::from(k.as_str()))
            .collect::<Vec<_>>();
        let values = options
            .iter()
            .map(|(_, v)| duckdb_v2_str::from(v.as_str()))
            .collect::<Vec<_>>();
        unsafe {
            let mut handle = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_instance_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_instance_create_with_options(
                    *env.env,
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
                    options.len() as idx_t,
                    &mut *handle,
                    err,
                )
            })?;
            let path_str = duckdb_v2_str::from(path);
            check_rc(|err| {
                duckdb_v2_instance_attach(
                    *handle,
                    ptr::null(),
                    &path_str,
                    ptr::null_mut(),
                    true,
                    err,
                )
            })?;
            Ok(Arc::new(DuckDBInstance { handle }))
        }
    }

    #[inline(always)]
    unsafe fn write_v2_bytes(
        vector: duckdb_v2_vector_handle,
        arena: &mut duckdb_v2_arena_handle,
        dst: *mut duckdb_v2_bytes,
        bytes: &[u8],
    ) -> Result<()> {
        unsafe {
            let len = bytes.len();
            let mut raw: duckdb_v2_bytes = mem::zeroed();
            if len <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
                raw.value.inlined.length = len as u32;
                ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    raw.value.inlined.inlined.as_mut_ptr() as *mut u8,
                    len,
                );
            } else {
                if arena.is_null() {
                    check_rc(|err| duckdb_v2_vector_get_arena(vector, arena, err))?;
                }
                let mut buf: *mut u8 = ptr::null_mut();
                check_rc(|err| duckdb_v2_arena_allocate(*arena, len as idx_t, &mut buf, err))?;
                ptr::copy_nonoverlapping(bytes.as_ptr(), buf, len);
                raw.value.pointer.length = len as u32;
                ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    raw.value.pointer.prefix.as_mut_ptr() as *mut u8,
                    4,
                );
                raw.value.pointer.ptr = buf as *mut _;
            }
            *dst = raw;
            Ok(())
        }
    }

    unsafe fn write_column_batch(
        factory: duckdb_v2_factory_handle,
        vector: duckdb_v2_vector_handle,
        col_idx: usize,
        direct_write: bool,
        batch: &[Vec<Value<'static>>],
    ) -> Result<()> {
        unsafe {
            check_rc(|err| duckdb_v2_vector_set_size(vector, batch.len() as idx_t, err))?;
            let mut data = ptr::null_mut();
            if direct_write {
                check_rc(|err| duckdb_v2_vector_get_data_mutable(vector, &mut data, err))?;
            }
            let mut arena: duckdb_v2_arena_handle = ptr::null_mut();
            for (row_idx, row) in batch.iter().enumerate() {
                let val = &row[col_idx];
                if val.is_null() {
                    check_rc(|err| duckdb_v2_vector_set_null(vector, row_idx as idx_t, err))?;
                    continue;
                }
                if direct_write {
                    match val {
                        Value::Boolean(Some(v)) => *(data as *mut bool).add(row_idx) = *v,
                        Value::Int8(Some(v)) => *(data as *mut i8).add(row_idx) = *v,
                        Value::Int16(Some(v)) => *(data as *mut i16).add(row_idx) = *v,
                        Value::Int32(Some(v)) => *(data as *mut i32).add(row_idx) = *v,
                        Value::Int64(Some(v)) => *(data as *mut i64).add(row_idx) = *v,
                        Value::Int128(Some(v)) => {
                            *(data as *mut duckdb_v2_hugeint_t).add(row_idx) =
                                i128_to_duckdb_hugeint(**v)
                        }
                        Value::UInt8(Some(v)) => *(data as *mut u8).add(row_idx) = *v,
                        Value::UInt16(Some(v)) => *(data as *mut u16).add(row_idx) = *v,
                        Value::UInt32(Some(v)) => *(data as *mut u32).add(row_idx) = *v,
                        Value::UInt64(Some(v)) => *(data as *mut u64).add(row_idx) = *v,
                        Value::UInt128(Some(v)) => {
                            *(data as *mut duckdb_v2_uhugeint_t).add(row_idx) =
                                u128_to_duckdb_uhugeint(**v)
                        }
                        Value::Float32(Some(v)) => *(data as *mut f32).add(row_idx) = *v,
                        Value::Float64(Some(v)) => *(data as *mut f64).add(row_idx) = *v,
                        Value::Date(Some(v)) => {
                            *(data as *mut i32).add(row_idx) = date_to_duckdb_date(v)
                        }
                        Value::Time(Some(v)) => {
                            *(data as *mut i64).add(row_idx) = time_to_duckdb_time(v)
                        }
                        Value::Timestamp(Some(v)) => {
                            *(data as *mut i64).add(row_idx) =
                                primitive_date_time_to_duckdb_timestamp(v)
                        }
                        Value::TimestampWithTimezone(Some(v)) => {
                            *(data as *mut i64).add(row_idx) = offsetdatetime_to_duckdb_timestamp(v)
                        }
                        Value::Interval(Some(v)) => {
                            *(data as *mut duckdb_v2_interval_t).add(row_idx) =
                                interval_to_duckdb_interval(v)
                        }
                        Value::Uuid(Some(v)) => {
                            *(data as *mut duckdb_v2_hugeint_t).add(row_idx) =
                                uuid_to_duckdb_hugeint(v)
                        }
                        Value::Char(Some(v)) => {
                            let mut buf = [0u8; 4];
                            let s = v.encode_utf8(&mut buf);
                            Self::write_v2_bytes(
                                vector,
                                &mut arena,
                                (data as *mut duckdb_v2_bytes).add(row_idx),
                                s.as_bytes(),
                            )?;
                        }
                        Value::Varchar(Some(v)) => {
                            Self::write_v2_bytes(
                                vector,
                                &mut arena,
                                (data as *mut duckdb_v2_bytes).add(row_idx),
                                v.as_bytes(),
                            )?;
                        }
                        Value::Blob(Some(v)) => {
                            Self::write_v2_bytes(
                                vector,
                                &mut arena,
                                (data as *mut duckdb_v2_bytes).add(row_idx),
                                v.as_ref(),
                            )?;
                        }
                        _ => {
                            let d_val = ValueWrap::from(val).to_duckdb_value(factory)?;
                            check_rc(|err| {
                                duckdb_v2_vector_set_value(vector, row_idx as idx_t, *d_val, err)
                            })?;
                        }
                    }
                } else {
                    let d_val = ValueWrap::from(val).to_duckdb_value(factory)?;
                    check_rc(|err| {
                        duckdb_v2_vector_set_value(vector, row_idx as idx_t, *d_val, err)
                    })?;
                }
            }
            Ok(())
        }
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
        let mut path = format!(
            "{}{}",
            url.host_str()
                .map_or(Default::default(), |host| format!("{host}/")),
            url.path()
        );
        if path.is_empty() {
            path = ":memory:".to_owned();
        }
        let mut options = Vec::new();
        for (key, value) in url.query_pairs() {
            match &*key {
                "mode" => {
                    if value == "memory" {
                        path = ":memory:".to_owned();
                        continue;
                    }
                    let access_mode = match &*value {
                        "ro" => "READ_ONLY",
                        "rw" | "rwc" => "READ_WRITE",
                        _ => {
                            let error = anyhow!(
                                "Unknown value {value:?} for `mode`, expected one of: `ro`, `rw`, `rwc`, `memory`"
                            );
                            log::warn!("{error:#}");
                            return Err(error);
                        }
                    };
                    options.push(("access_mode".to_owned(), access_mode.to_owned()));
                }
                _ => options.push((key.into_owned(), value.into_owned())),
            }
        }
        let is_memory = path == ":memory:";
        let instance = if is_memory {
            Self::create_instance(&path, &options).with_context(make_context)?
        } else {
            let mut cache = Self::env().instances.lock().unwrap();
            if let Some(existing) = cache.get(&path).and_then(Weak::upgrade) {
                existing
            } else {
                let created = Self::create_instance(&path, &options).with_context(make_context)?;
                cache.insert(path.clone(), Arc::downgrade(&created));
                created
            }
        };
        unsafe {
            let mut connection = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_connection_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_connection_create(*instance.handle, &mut *connection, err))
                .with_context(make_context)?;
            let mut factory = ptr::null_mut();
            check_rc(|err| duckdb_v2_connection_get_factory(*connection, &mut factory, err))
                .with_context(make_context)?;
            if !options
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("max_streaming_buffer_size"))
            {
                let mut config = ptr::null_mut();
                check_rc(|err| duckdb_v2_connection_get_config(*connection, &mut config, err))
                    .with_context(make_context)?;
                let opt_name = duckdb_v2_str::from("max_streaming_buffer_size");
                let opt_val = duckdb_v2_str::from("1B");
                check_rc(|err| {
                    duckdb_v2_config_set_option_text(
                        config,
                        &opt_name,
                        &opt_val,
                        DUCKDB_V2_SETTING_SCOPE::DUCKDB_V2_SETTING_SCOPE_SESSION,
                        err,
                    )
                })
                .with_context(make_context)?;
            }
            Ok(DuckDBConnection {
                connection,
                factory,
                _instance: instance,
            })
        }
    }

    fn begin(&mut self) -> impl Future<Output = Result<DuckDBTransaction<'_>>> + Send {
        DuckDBTransaction::new(self)
    }
}

impl Executor for DuckDBConnection {
    type Driver = DuckDBDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<DuckDBDriver>> {
        let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
        let c_sql = CString::new(sql.as_bytes())
            .context("Could not convert `String` to `CString`")
            .with_context(make_context)
            .inspect_err(|error| log::error!("{error:#}"))?;
        unsafe {
            let mut iterator = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_statement_iterator_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_parse_sql(*self.connection, c_sql.as_ptr(), &mut *iterator, err)
            })
            .with_context(make_context)
            .inspect_err(|error| log::error!("{error:#}"))?;
            let mut parsed = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_sql_statement_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_statement_iterator_next(*iterator, &mut *parsed, err))
                .with_context(make_context)
                .inspect_err(|error| log::error!("{error:#}"))?;
            if parsed.is_null() {
                let error = anyhow!("No statement to prepare").context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            let mut extra = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_sql_statement_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_statement_iterator_next(*iterator, &mut *extra, err))
                .with_context(make_context)
                .inspect_err(|error| log::error!("{error:#}"))?;
            if !extra.is_null() {
                let error =
                    anyhow!("Cannot prepare multiple statements at once").context(make_context());
                log::error!("{error:#}");
                return Err(error);
            }
            let mut param_count = 0;
            check_rc(|err| {
                duckdb_v2_sql_statement_get_parameter_count(*parsed, &mut param_count, err)
            })
            .with_context(make_context)
            .inspect_err(|error| log::error!("{error:#}"))?;
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
            })
            .with_context(make_context)
            .inspect_err(|error| log::error!("{error:#}"))?;
            Ok(Query::Prepared(DuckDBPrepared::new(
                prepared,
                param_count,
                sql,
            )))
        }
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
        let rows = rows
            .into_iter()
            .map(|e| e.as_entity().row_values())
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(Default::default());
        }
        let columns = E::<It>::columns();
        if columns.is_empty() {
            return Err(anyhow!("Cannot bulk append an entity without columns"));
        }
        let (_collection, _stmt, result) = unsafe {
            let factory = self.factory;
            let mut col_types = Vec::with_capacity(columns.len());
            let mut direct_flags = Vec::with_capacity(columns.len());
            for col in columns {
                if let Some(override_ty) = col
                    .column_type
                    .get("duckdb")
                    .or_else(|| col.column_type.get(""))
                {
                    let text = duckdb_v2_str::from(*override_ty);
                    let mut out = CBox::new(ptr::null_mut(), |mut p| {
                        duckdb_v2_logical_type_destroy(&mut p);
                    });
                    check_rc(|err| {
                        duckdb_v2_factory_create_type_from_text(factory, &text, &mut *out, err)
                    })?;
                    col_types.push(out);
                    direct_flags.push(false);
                } else {
                    col_types.push(ValueWrap::from(&col.value).to_logical_type(factory)?);
                    direct_flags.push(!matches!(
                        col.value,
                        Value::Decimal(..)
                            | Value::List(..)
                            | Value::Array(..)
                            | Value::Map(..)
                            | Value::Struct(..)
                            | Value::Json(..)
                            | Value::Null
                            | Value::Unknown(..)
                    ));
                }
            }
            let raw_types = col_types.iter().map(|t| **t).collect::<Vec<_>>();
            let mut collection = CBox::new(ptr::null_mut(), |mut c| {
                duckdb_v2_column_data_collection_destroy(&mut c);
            });
            check_rc(|err| {
                duckdb_v2_column_data_collection_create(
                    factory,
                    raw_types.as_ptr(),
                    raw_types.len() as idx_t,
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
                    raw_types.len() as idx_t,
                    &mut *chunk,
                    err,
                )
            })?;
            let mut capacity = 0;
            check_rc(|err| duckdb_v2_data_chunk_get_capacity(*chunk, &mut capacity, err))?;
            let batch_cap = (capacity as usize).max(1);
            for batch in rows.chunks(batch_cap) {
                check_rc(|err| duckdb_v2_data_chunk_clear(*chunk, err))?;
                for col_idx in 0..columns.len() {
                    let mut vector = ptr::null_mut();
                    check_rc(|err| {
                        duckdb_v2_data_chunk_get_vector(*chunk, col_idx as idx_t, &mut vector, err)
                    })?;
                    Self::write_column_batch(
                        factory,
                        vector,
                        col_idx,
                        direct_flags[col_idx],
                        batch,
                    )?;
                }
                check_rc(|err| {
                    duckdb_v2_column_data_collection_append(*collection, *append_state, *chunk, err)
                })?;
            }
            let writer = self.driver().sql_writer();
            let mut context = Context::new(Fragment::SqlInsertInto, false);
            let mut query = DynQuery::default();
            query.push_str("INSERT INTO ");
            writer.write_table_ref(&mut context, &mut query, E::<It>::table());
            query.push_str(" (");
            separated_by(
                &mut query,
                columns,
                |out, col| {
                    let quote = context.quote_identifiers;
                    writer.write_identifier(&mut context, out, col.name(), quote);
                },
                ", ",
            );
            query.push_str(") SELECT * FROM ");
            writer.write_identifier(&mut context, &mut query, "__tank_append", true);
            let c_sql = CString::new(query.as_str().as_bytes()).map_err(Error::new)?;
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
            let cte_name = duckdb_v2_str::from("__tank_append");
            check_rc(|err| {
                duckdb_v2_statement_add_collection(
                    *stmt,
                    &cte_name,
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
            (collection, stmt, result)
        };
        let mut rows_changed = None;
        poll_fn(|cx| {
            let mut steps = 0;
            Statements::<&mut Query<DuckDBDriver>>::poll_affected(
                *result,
                &mut rows_changed,
                &mut steps,
                cx,
            )
        })
        .await
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
