use crate::{DuckDBConnection, DuckDBDriver, cbox::CBox, extract::extract_value};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    ffi::{CStr, CString},
    mem, ptr,
    sync::{Arc, Mutex},
};
use tank_core::{
    Error, Query, QueryResult, Result, RowLabels, RowOwned, RowsAffected, error_message_from_ptr,
};

// Chunk storage is released before its result. Rows returned to callers own their values.
struct NativeRows {
    chunk: Option<CBox<duckdb_data_chunk>>,
    result: CBox<duckdb_result>,
    names: RowLabels,
    types: Vec<(CBox<duckdb_logical_type>, u32)>,
    position: u64,
    streaming: bool,
}

impl NativeRows {
    fn new(mut result: CBox<duckdb_result>) -> Result<Self> {
        unsafe {
            let cols = duckdb_column_count(&mut *result);
            let names = (0..cols)
                .map(|i| {
                    let name = duckdb_column_name(&mut *result, i);
                    if name.is_null() {
                        return Err(anyhow!("Missing DuckDB column name at index {i}"));
                    }
                    Ok(CStr::from_ptr(name).to_str()?.to_owned())
                })
                .collect::<Result<RowLabels>>()?;
            let types = (0..cols)
                .map(|i| {
                    let ty = CBox::new(duckdb_column_logical_type(&mut *result, i), |mut ty| {
                        duckdb_destroy_logical_type(&mut ty)
                    });
                    if ty.is_null() {
                        return Err(anyhow!("Missing DuckDB column type at index {i}"));
                    }
                    let id = duckdb_get_type_id(*ty);
                    Ok((ty, id))
                })
                .collect::<Result<_>>()?;
            let streaming = duckdb_result_is_streaming(*result);
            Ok(Self {
                chunk: None,
                result,
                names,
                types,
                position: 0,
                streaming,
            })
        }
    }

    fn next(&mut self) -> Result<Option<RowOwned>> {
        unsafe {
            loop {
                if let Some(chunk) = &self.chunk {
                    if self.position < duckdb_data_chunk_get_size(**chunk) {
                        let values = self
                            .types
                            .iter()
                            .enumerate()
                            .map(|(col, (ty, id))| {
                                let vector = duckdb_data_chunk_get_vector(**chunk, col as u64);
                                extract_value(
                                    vector,
                                    self.position as usize,
                                    **ty,
                                    *id,
                                    duckdb_vector_get_data(vector),
                                    duckdb_vector_get_validity(vector),
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        self.position += 1;
                        if self.position == duckdb_data_chunk_get_size(**chunk) {
                            self.chunk = None;
                        }
                        return Ok(Some(RowOwned::new(self.names.clone(), values)));
                    }
                }
                // Free the previous chunk before fetching another; never queue rows or chunks.
                self.chunk = None;
                let chunk = CBox::new(
                    if self.streaming {
                        duckdb_stream_fetch_chunk(*self.result)
                    } else {
                        duckdb_fetch_chunk(*self.result)
                    },
                    |mut chunk| duckdb_destroy_data_chunk(&mut chunk),
                );
                if chunk.is_null() {
                    let error = duckdb_result_error(&mut *self.result);
                    return if error.is_null() {
                        Ok(None)
                    } else {
                        Err(Error::msg(
                            CStr::from_ptr(error).to_string_lossy().into_owned(),
                        ))
                    };
                }
                self.position = 0;
                self.chunk = Some(chunk);
            }
        }
    }
}

struct Execution<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    rows: Option<NativeRows>,
    prepared: Option<CBox<duckdb_prepared_statement>>,
    extracted: Option<CBox<duckdb_extracted_statements>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    context: String,
    count: u64,
    index: u64,
    generation: u64,
    finished: bool,
}

enum Statement {
    Rows,
    Affected(RowsAffected),
}

impl<Q: AsMut<Query<DuckDBDriver>>> Execution<'_, Q> {
    fn finish(&mut self) -> Result<()> {
        self.rows = None;
        self.prepared = None;
        self.extracted = None;
        if !self.finished {
            self.finished = true;
            self.query.as_mut().clear_bindings()?;
        }
        Ok(())
    }

    fn fail(&mut self, error: Error) -> Error {
        if let Err(cleanup) = self.finish() {
            log::error!("While cleaning up DuckDB execution: {cleanup:#}");
        }
        error.context(self.context.clone())
    }

    fn next_row(&mut self) -> Result<Option<RowOwned>> {
        let Some(rows) = self.rows.as_mut() else {
            return Ok(None);
        };
        match rows.next() {
            Ok(Some(row)) => Ok(Some(row)),
            Ok(None) => {
                self.rows = None;
                self.prepared = None;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn next_statement(&mut self) -> Result<Option<Statement>> {
        if self.finished {
            return Ok(None);
        }
        // An outer advance invalidates the previous row iterator, but does not hide errors
        // in unread rows. Extract and discard one row at a time before executing more SQL.
        while self.next_row()?.is_some() {}
        if self.index == self.count {
            self.finish()?;
            return Ok(None);
        }
        unsafe {
            let statement = if let Some(extracted) = &self.extracted {
                let mut prepared =
                    CBox::new(ptr::null_mut(), |mut p| duckdb_destroy_prepare(&mut p));
                let rc = duckdb_prepare_extracted_statement(
                    *self.connection.connection,
                    **extracted,
                    self.index,
                    &mut *prepared,
                );
                if rc != duckdb_state_DuckDBSuccess {
                    return Err(Error::msg(
                        error_message_from_ptr(&duckdb_prepare_error(*prepared)).into_owned(),
                    ));
                }
                let statement = *prepared;
                self.prepared = Some(prepared);
                statement
            } else {
                match self.query.as_mut() {
                    Query::Prepared(query) => query.statement(),
                    Query::Raw(_) => unreachable!(),
                }
            };
            self.index += 1;
            self.generation += 1;
            let mut result = CBox::new(mem::zeroed(), |mut r| duckdb_destroy_result(&mut r));
            if duckdb_execute_prepared_streaming(statement, &mut *result)
                != duckdb_state_DuckDBSuccess
            {
                return Err(Error::msg(
                    error_message_from_ptr(&duckdb_result_error(&mut *result)).into_owned(),
                ));
            }
            if duckdb_result_return_type(*result)
                == duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT
            {
                self.rows = Some(NativeRows::new(result)?);
                Ok(Some(Statement::Rows))
            } else {
                let affected = RowsAffected {
                    rows_affected: Some(duckdb_rows_changed(&mut *result)),
                    ..Default::default()
                };
                // The native result must die before its temporary prepared statement.
                drop(result);
                self.prepared = None;
                Ok(Some(Statement::Affected(affected)))
            }
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for Execution<'_, Q> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            log::error!("While dropping DuckDB execution: {error:#}");
        }
    }
}

pub struct Statements<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Both iterators may move between threads, but all native access is serialized.
    // The shared state retains the exclusive connection/query borrows until both die.
    execution: Arc<Mutex<Execution<'s, Q>>>,
}

pub struct Rows<'s, Q: AsMut<Query<DuckDBDriver>>> {
    execution: Arc<Mutex<Execution<'s, Q>>>,
    generation: u64,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Statements<'s, Q> {
    pub(crate) fn new(connection: &'s mut DuckDBConnection, mut query: Q) -> Result<Self> {
        let context = format!("While running the query:\n{}", query.as_mut());
        let mut count = 1;
        let extracted = if let Query::Raw(raw) = query.as_mut() {
            let sql = CString::new(raw.sql.as_bytes())
                .map_err(|error| Error::new(error).context(context.clone()))?;
            let mut extracted = CBox::new(ptr::null_mut(), |mut p| unsafe {
                duckdb_destroy_extracted(&mut p)
            });
            unsafe {
                count = duckdb_extract_statements(
                    *connection.connection,
                    sql.as_ptr(),
                    &mut *extracted,
                );
                let error = duckdb_extract_statements_error(*extracted);
                if !error.is_null() {
                    return Err(
                        Error::msg(CStr::from_ptr(error).to_string_lossy().into_owned())
                            .context(context),
                    );
                }
            }
            Some(extracted)
        } else {
            None
        };
        Ok(Self {
            execution: Arc::new(Mutex::new(Execution {
                rows: None,
                prepared: None,
                extracted,
                query,
                connection,
                context,
                count,
                index: 0,
                generation: 0,
                finished: false,
            })),
        })
    }
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Iterator for Statements<'s, Q> {
    type Item = Result<QueryResult<Rows<'s, Q>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut execution = self.execution.lock().unwrap();
        match execution.next_statement() {
            Ok(Some(Statement::Rows)) => Some(Ok(QueryResult::Rows(Rows {
                execution: Arc::clone(&self.execution),
                generation: execution.generation,
            }))),
            Ok(Some(Statement::Affected(affected))) => Some(Ok(QueryResult::Affected(affected))),
            Ok(None) => None,
            Err(error) => Some(Err(execution.fail(error))),
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Iterator for Rows<'_, Q> {
    type Item = Result<RowOwned>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut execution = self.execution.lock().unwrap();
        if self.generation != execution.generation {
            return None;
        }
        match execution.next_row() {
            Ok(row) => row.map(Ok),
            Err(error) => Some(Err(execution.fail(error))),
        }
    }
}
