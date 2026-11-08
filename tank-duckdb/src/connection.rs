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
    cmp::{max, min},
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
    separated_by,
    stream::{self, Stream, TryStreamExt},
    truncate_long,
};

#[derive(Debug)]
pub(crate) struct DuckDBInstance {
    pub(crate) handle: CBox<duckdb_v2_instance_handle>,
}

struct DuckDBEnvironment {
    handle: CBox<duckdb_v2_environment_handle>,
    instances: Mutex<HashMap<String, Weak<DuckDBInstance>>>,
}

/// Wrapper around a DuckDB `duckdb_v2_connection_handle`.
/// Provides helpers to execute queries and extract results into `tank_core` types.
pub struct DuckDBConnection {
    pub(crate) connection: CBox<duckdb_v2_connection_handle>,
    pub(crate) factory: duckdb_v2_factory_handle,
    pub(crate) labels: RowLabels,
    pub(crate) _instance: Arc<DuckDBInstance>,
}

impl DuckDBConnection {
    fn environment() -> Result<&'static DuckDBEnvironment> {
        static ENVIRONMENT: OnceLock<DuckDBEnvironment> = OnceLock::new();
        if let Some(env) = ENVIRONMENT.get() {
            return Ok(env);
        }
        let mut environment = CBox::new(ptr::null_mut(), |mut p| unsafe {
            duckdb_v2_environment_destroy(&mut p);
        });
        unsafe {
            check_rc(|err| duckdb_v2_environment_create(&mut *environment, err))
                .context("While creating the DuckDB environment")?;
        }
        let _ = ENVIRONMENT.set(DuckDBEnvironment {
            handle: environment,
            instances: Default::default(),
        });
        ENVIRONMENT
            .get()
            .context("While accessing the DuckDB environment")
    }

    fn new(path: &str, options: &[(String, String)]) -> Result<Arc<DuckDBInstance>> {
        let environment = Self::environment()?;
        let names: Vec<_> = options.iter().map(|(k, _)| k.into()).collect();
        let values: Vec<_> = options.iter().map(|(_, v)| v.into()).collect();
        unsafe {
            let mut instance = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_instance_destroy(&mut p);
            });
            check_rc(|err| {
                duckdb_v2_instance_create_with_options(
                    *environment.handle,
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
                    &mut *instance,
                    err,
                )
            })?;
            let path: duckdb_v2_str = path.into();
            check_rc(|err| {
                duckdb_v2_instance_attach(*instance, ptr::null(), &path, ptr::null_mut(), true, err)
            })?;
            Ok(Arc::new(DuckDBInstance { handle: instance }))
        }
    }

    #[inline(always)]
    fn write_duckdb_bytes(
        vector: duckdb_v2_vector_handle,
        dst: &mut duckdb_v2_bytes,
        src: &[u8],
    ) -> Result<()> {
        let len = src.len();
        unsafe {
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
                check_rc(|err| duckdb_v2_vector_get_arena(vector, &mut arena, err))?;
                check_rc(|err| duckdb_v2_arena_allocate(arena, len as idx_t, &mut buf, err))?;
                bytes.value.pointer.length = len as u32;
                ptr::copy_nonoverlapping(src.as_ptr(), buf, len);
                ptr::copy_nonoverlapping(
                    src.as_ptr(),
                    bytes.value.pointer.prefix.as_mut_ptr() as _,
                    4,
                );
                bytes.value.pointer.ptr = buf as *mut _;
            }
            *dst = bytes;
        }
        Ok(())
    }

    #[inline(always)]
    fn write_cell(
        factory: duckdb_v2_factory_handle,
        vector: duckdb_v2_vector_handle,
        col_val: &Value<'static>,
        direct_write: bool,
        data: *mut c_void,
        row_idx: usize,
        val: &Value<'_>,
    ) -> Result<()> {
        unsafe {
            if val.is_null() {
                return check_rc(|err| duckdb_v2_vector_set_null(vector, row_idx as idx_t, err));
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
                    Value::Decimal(Some(v), ..) => {
                        if let Value::Decimal(_, width, scale) = col_val {
                            let scale = min(*scale, 38);
                            let width = max(*width, scale).clamp(1, 38);
                            let d_scale = v.scale() as u8;
                            let scaled = if d_scale <= scale {
                                v.mantissa()
                                    .saturating_mul(10_i128.pow((scale - d_scale) as u32))
                            } else {
                                let divisor = 10_i128.pow((d_scale - scale) as u32);
                                let rem = v.mantissa() % divisor;
                                let quot = v.mantissa() / divisor;
                                if rem.abs() * 2 >= divisor {
                                    quot + v.mantissa().signum()
                                } else {
                                    quot
                                }
                            };
                            if width <= 4 {
                                *(data as *mut i16).add(row_idx) = scaled as i16;
                            } else if width <= 9 {
                                *(data as *mut i32).add(row_idx) = scaled as i32;
                            } else if width <= 18 {
                                *(data as *mut i64).add(row_idx) = scaled as i64;
                            } else {
                                *(data as *mut duckdb_v2_hugeint_t).add(row_idx) =
                                    i128_to_duckdb_hugeint(scaled);
                            }
                        }
                    }
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
                        *(data as *mut duckdb_v2_hugeint_t).add(row_idx) = uuid_to_duckdb_hugeint(v)
                    }
                    Value::Char(Some(v)) => {
                        let mut buf = [0u8; 4];
                        let s = v.encode_utf8(&mut buf);
                        Self::write_duckdb_bytes(
                            vector,
                            &mut *(data as *mut duckdb_v2_bytes).add(row_idx),
                            s.as_bytes(),
                        )?;
                    }
                    Value::Varchar(Some(v)) => {
                        Self::write_duckdb_bytes(
                            vector,
                            &mut *(data as *mut duckdb_v2_bytes).add(row_idx),
                            v.as_bytes(),
                        )?;
                    }
                    Value::Blob(Some(v)) => {
                        Self::write_duckdb_bytes(
                            vector,
                            &mut *(data as *mut duckdb_v2_bytes).add(row_idx),
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
                check_rc(|err| duckdb_v2_vector_set_value(vector, row_idx as idx_t, *d_val, err))?;
            }
            Ok(())
        }
    }
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let context = "While trying to connect to DuckDB";
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
                    options.push(("access_mode".to_owned(), access_mode.to_owned()));
                }
                _ => options.push((k.into_owned(), v.into_owned())),
            }
        }
        let is_memory = path == ":memory:";
        let instance = if is_memory {
            Self::new(&path, &options).context(context)?
        } else {
            let mut cache = Self::environment()
                .context(context)?
                .instances
                .lock()
                .ok()
                .context("DuckDB instance cache mutex was poisoned")
                .context(context)?;
            if let Some(existing) = cache.get(&path).and_then(Weak::upgrade) {
                existing
            } else {
                let created = Self::new(&path, &options).context(context)?;
                cache.insert(path.clone(), Arc::downgrade(&created));
                created
            }
        };
        unsafe {
            let mut connection = CBox::new(ptr::null_mut(), |mut p| {
                duckdb_v2_connection_destroy(&mut p);
            });
            check_rc(|err| duckdb_v2_connection_create(*instance.handle, &mut *connection, err))
                .context(context)?;
            let mut factory = ptr::null_mut();
            check_rc(|err| duckdb_v2_connection_get_factory(*connection, &mut factory, err))
                .context(context)?;
            if !options
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("max_streaming_buffer_size"))
            {
                let mut config = ptr::null_mut();
                check_rc(|err| duckdb_v2_connection_get_config(*connection, &mut config, err))
                    .context(context)?;
                let opt_name: duckdb_v2_str = "max_streaming_buffer_size".into();
                let opt_val: duckdb_v2_str = "256KiB".into();
                check_rc(|err| {
                    duckdb_v2_config_set_option_text(
                        config,
                        &opt_name,
                        &opt_val,
                        DUCKDB_V2_SETTING_SCOPE::DUCKDB_V2_SETTING_SCOPE_SESSION,
                        err,
                    )
                })
                .context(context)?;
            }
            Ok(DuckDBConnection {
                connection,
                factory,
                labels: RowLabels::default(),
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
        let columns = E::<It>::columns();
        if columns.is_empty() {
            return Err(anyhow!("Cannot bulk append an entity without columns"));
        }
        let Some((_collection, result)) = (unsafe {
            let mut iter = rows.into_iter().peekable();
            if iter.peek().is_none() {
                None
            } else {
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
                            Value::Decimal(_, 0, _)
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
                let batch_cap = max(capacity as usize, 1);
                let mut col_states = Vec::with_capacity(columns.len());
                for (col_idx, (&direct_write, col)) in
                    direct_flags.iter().zip(columns.iter()).enumerate()
                {
                    let mut vector = ptr::null_mut();
                    check_rc(|err| {
                        duckdb_v2_data_chunk_get_vector(*chunk, col_idx as idx_t, &mut vector, err)
                    })?;
                    col_states.push((
                        vector,
                        &col.value,
                        direct_write,
                        ptr::null_mut::<c_void>(),
                    ));
                }
                loop {
                    let Some(first) = iter.next() else {
                        break;
                    };
                    check_rc(|err| duckdb_v2_data_chunk_clear(*chunk, err))?;
                    for (vector, _, direct_write, data) in &mut col_states {
                        check_rc(|err| {
                            duckdb_v2_vector_set_size(*vector, batch_cap as idx_t, err)
                        })?;
                        if *direct_write {
                            check_rc(|err| duckdb_v2_vector_get_data_mutable(*vector, data, err))?;
                        }
                    }
                    let mut batch_len = 0;
                    for item in std::iter::once(first).chain(iter.by_ref().take(batch_cap - 1)) {
                        let row = item.as_entity().row_values();
                        for (col_idx, (vector, col_val, direct_write, data)) in
                            col_states.iter_mut().enumerate()
                        {
                            let val = row.get(col_idx).with_context(|| {
                                format!("Missing value at column {col_idx} in row {batch_len}")
                            })?;
                            Self::write_cell(
                                factory,
                                *vector,
                                col_val,
                                *direct_write,
                                *data,
                                batch_len,
                                val,
                            )?;
                        }
                        batch_len += 1;
                    }
                    if batch_len < batch_cap {
                        for (vector, ..) in &col_states {
                            check_rc(|err| {
                                duckdb_v2_vector_set_size(*vector, batch_len as idx_t, err)
                            })?;
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
                Some((collection, result))
            }
        }) else {
            return Ok(Default::default());
        };
        let mut statements = Statements::new(self, Query::<DuckDBDriver>::default())?;
        statements.result = Some(result);
        match statements.try_next().await? {
            Some(QueryResult::Affected(affected)) => Ok(affected),
            _ => Ok(Default::default()),
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
