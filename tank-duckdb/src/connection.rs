use crate::{
    CBox, DuckDBDriver, DuckDBPrepared, DuckDBTransaction, batch::Batch, column::Column, ensure_rc,
    stream::DuckDBStream,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    ffi::{CStr, CString, c_char},
    future::{self, Future},
    mem, ptr,
    sync::{Arc, LazyLock},
};
use tank_core::{
    AsEntity, AsQuery, Connection, Entity, Error, ErrorContext, Executor, QueryResult, Result, Row,
    RowsAffected, as_c_string, describe_path, error_message_from_ptr, stream::Stream,
    truncate_long,
};

/// Wrapper around a DuckDB `duckdb_connection` pointer used by the DuckDB driver.
#[derive(Debug)]
pub struct DuckDBConnection {
    // Drop order: disconnect before closing the database handle.
    pub(crate) connection: CBox<duckdb_connection>,
    _database: CBox<duckdb_database>,
    pub(crate) batch: Option<Arc<Batch>>,
}

impl DuckDBConnection {
    #[inline(always)]
    pub(crate) fn do_execute(&mut self, statement: &CStr) -> Result<()> {
        let mut result = CBox(unsafe { mem::zeroed::<duckdb_result>() });
        ensure_rc!(
            unsafe { duckdb_query(*self.connection, statement.as_ptr(), &mut *result) },
            Error::msg(
                error_message_from_ptr(&unsafe { duckdb_result_error(&mut *result) }).into_owned()
            ),
            format!(
                "While executing:\n{}",
                truncate_long(&statement.to_string_lossy()),
            )
        );
        Ok(())
    }
}

impl Connection for DuckDBConnection {
    async fn connect(driver: &DuckDBDriver, url: Cow<'static, str>) -> Result<Self> {
        static DATABASE_CACHE: LazyLock<CBox<duckdb_instance_cache>> =
            LazyLock::new(|| CBox(unsafe { duckdb_create_instance_cache() }));
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || format!(r#"Connecting to DuckDB "{}""#, describe_path(&url));
        let mut config = CBox::<duckdb_config>::null();
        ensure_rc!(
            unsafe { duckdb_create_config(&mut *config) },
            anyhow!("Cannot allocate the duckdb_config object").context(make_context())
        );
        let mut path = match url.host_str() {
            Some(host) => CString::new(format!("{host}/{}", url.path())),
            None => CString::new(url.path()),
        }
        .with_context(make_context)?;
        for (k, v) in url.query_pairs() {
            let rc = unsafe {
                match &*k {
                    "mode" => {
                        if v == "memory" {
                            path = c":memory:".to_owned();
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
                                    log::error!("{error:#}");
                                    return Err(error);
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
                Error::msg(error_message_from_ptr(&(*error as *const c_char)).into_owned()),
                "While reading the error from `duckdb_get_or_create_from_cache`"
            );
            ensure_rc!(
                duckdb_connect(*database, &mut *connection),
                anyhow!("Failed to connect to database url `{url}`"),
            );
        }
        Ok(Self {
            connection,
            _database: database,
            batch: None,
        })
    }

    #[inline(always)]
    fn begin(&mut self) -> impl Future<Output = Result<DuckDBTransaction<'_>>> + Send {
        future::ready(DuckDBTransaction::new(self))
    }
}

impl Executor for DuckDBConnection {
    type Driver = DuckDBDriver;

    fn do_prepare(&mut self, sql: &str) -> impl Future<Output = Result<DuckDBPrepared>> + Send {
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
                Ok(DuckDBPrepared::new(prepared))
            })()
            .with_context(|| format!("While preparing the query:\n{}", truncate_long(sql)))
            .inspect_err(|error| log::error!("{error:#}")),
        )
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<DuckDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        DuckDBStream::new(self, query.as_query(&self.driver()))
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
        let (catalog, schema) = match table.schema.rsplit_once('.') {
            Some((catalog, schema)) => (Some(catalog), Some(schema)),
            None => (None, (!table.schema.is_empty()).then_some(&*table.schema)),
        };
        future::ready((|| unsafe {
            let mut appender = CBox::null();
            let appender_error = |appender: duckdb_appender| {
                Error::msg(error_message_from_ptr(&duckdb_appender_error(appender)).into_owned())
            };
            let (catalog, schema, table) = (
                catalog.map(as_c_string),
                schema.map(as_c_string),
                as_c_string(&*table.name),
            );
            ensure_rc!(
                duckdb_appender_create_ext(
                    *self.connection,
                    catalog.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
                    schema.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
                    table.as_ptr(),
                    &mut *appender,
                ),
                appender_error(*appender).context("While creating the `duckdb_appender` object")
            );
            for column in columns {
                ensure_rc!(
                    duckdb_appender_add_column(*appender, as_c_string(column.name()).as_ptr()),
                    appender_error(*appender),
                    format!("While adding column {} to the appender", column.name()),
                );
            }
            let rows_affected = if !columns.is_empty()
                && columns.iter().all(|c| Column::supports_append(&c.value))
            {
                let mut types = columns
                    .iter()
                    .map(|c| CBox::<duckdb_logical_type>::from(&c.value))
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
                    .map(|i| Column::new(duckdb_data_chunk_get_vector(*chunk, i as _)))
                    .collect::<Box<[_]>>();
                let mut total = 0;
                loop {
                    duckdb_data_chunk_reset(*chunk);
                    columns.iter_mut().for_each(Column::reset_write);
                    let mut len = 0;
                    for entity in rows.by_ref().take(batch_size) {
                        for (value, column) in entity.as_entity().row_values().zip(&mut columns) {
                            column.append(len, value);
                        }
                        len += 1;
                    }
                    if len == 0 {
                        break total;
                    }
                    duckdb_data_chunk_set_size(*chunk, len as _);
                    ensure_rc!(
                        duckdb_append_data_chunk(*appender, *chunk),
                        appender_error(*appender),
                        "While appending data chunk",
                    );
                    total += len as u64;
                }
            } else {
                let res = append_values(*appender, rows);
                #[inline(never)]
                fn append_values<It>(appender: duckdb_appender, rows: It) -> Result<u64>
                where
                    It: IntoIterator,
                    It::Item: AsEntity,
                {
                    let mut total = 0;
                    let err = || {
                        Error::msg(
                            error_message_from_ptr(&unsafe { duckdb_appender_error(appender) })
                                .into_owned(),
                        )
                    };
                    for entity in rows {
                        for value in entity.as_entity().row_values() {
                            ensure_rc!(
                                unsafe {
                                    duckdb_append_value(
                                        appender,
                                        *CBox::<duckdb_value>::try_from(&value)?,
                                    )
                                },
                                err(),
                                format!("While appending {value:?}"),
                            );
                        }
                        ensure_rc!(
                            unsafe { duckdb_appender_end_row(appender) },
                            err(),
                            "While ending appender row",
                        );
                        total += 1;
                    }
                    Ok(total)
                }
                res?
            };
            ensure_rc!(
                duckdb_appender_close(*appender),
                appender_error(*appender),
                "While closing the `duckdb_appender`",
            );
            Ok(RowsAffected {
                rows_affected: Some(rows_affected),
                last_affected_id: None,
            })
        })())
    }
}
