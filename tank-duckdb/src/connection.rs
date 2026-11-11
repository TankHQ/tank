use crate::{
    DuckDBPrepared, DuckDBTransaction, ValueWrap, column::AppendColumn, driver::DuckDBDriver,
    ensure_rc, stream::DuckDBStream, util::CBox,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char},
    future::{self, Future},
    hint, ptr,
    str::FromStr,
    sync::LazyLock,
};
use tank_core::{
    AsEntity, AsQuery, Connection, Driver, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, Result, Row, RowsAffected, SqlWriter, as_c_string, describe_path,
    error_message_from_ptr, log_error, log_warn, stream::Stream, truncate_long,
};

/// Wrapper around a DuckDB `duckdb_connection` pointer used by the DuckDB driver.
#[derive(Debug)]
pub struct DuckDBConnection {
    // Drop order: disconnect before closing the database handle.
    pub(crate) connection: CBox<duckdb_connection>,
    _database: CBox<duckdb_database>,
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
            let (catalog, schema) = match table.schema.rsplit_once(delimiter) {
                Some((catalog, schema)) => (Some(catalog), Some(schema)),
                None => (None, Some(&*table.schema).filter(|s| !s.is_empty())),
            };
            let (catalog, schema) = (catalog.map(as_c_string), schema.map(as_c_string));
            let rc = duckdb_appender_create_ext(
                *self.connection,
                catalog.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
                schema.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
                as_c_string(&*table.name).as_ptr(),
                &mut *appender,
            );
            Self::check_appender(*appender, rc)
                .context("While creating the `duckdb_appender` object")?;
            for column in columns {
                Self::check_appender(
                    *appender,
                    duckdb_appender_add_column(*appender, as_c_string(column.name()).as_ptr()),
                )?;
            }
            let mut rows_affected = 0;
            if !columns.is_empty() && columns.iter().all(|c| AppendColumn::supports(&c.value)) {
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
                    .map(|i| AppendColumn::new(duckdb_data_chunk_get_vector(*chunk, i as _)))
                    .collect::<Box<[_]>>();
                loop {
                    duckdb_data_chunk_reset(*chunk);
                    columns.iter_mut().for_each(|c| c.reset());
                    let mut len = 0;
                    for entity in rows.by_ref().take(batch_size) {
                        for (value, column) in entity.as_entity().row_values().zip(&mut columns) {
                            column.append(len, value);
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
