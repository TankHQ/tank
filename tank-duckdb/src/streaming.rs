use crate::{
    DuckDBConnection, DuckDBDriver, ValueWrap,
    cbox::CBox,
    check_rc,
    extract::VectorNode,
    duckdb_str_to_str,
};
use anyhow::anyhow;
use libduckdb_sys::v2::*;
use std::{
    ffi::CString,
    mem,
    pin::Pin,
    ptr,
    task::{Context, Poll},
};
use tank_core::{
    Error, ErrorContext, Prepared, Query, QueryResult, Result, RowLabels, RowOwned, RowsAffected,
    ValueOwned, stream::Stream,
};

// Chunk storage is released before its result. Rows returned to callers own their values.
struct NativeRows {
    vectors: Vec<VectorNode>,
    chunk: Option<CBox<duckdb_v2_data_chunk_handle>>,
    result: CBox<duckdb_v2_result_handle>,
    names: RowLabels,
    types: Vec<ValueOwned>,
    chunk_size: usize,
    position: usize,
}

impl NativeRows {
    fn new(result: CBox<duckdb_v2_result_handle>) -> Result<Self> {
        unsafe {
            let mut schema = CBox::new(ptr::null_mut(), |mut s| {
                duckdb_v2_schema_destroy(&mut s);
            });
            check_rc(|err| duckdb_v2_result_get_schema(*result, &mut *schema, err))?;
            let mut cols = 0;
            check_rc(|err| duckdb_v2_schema_get_count(*schema, &mut cols, err))?;
            let mut names = Vec::with_capacity(cols as usize);
            let mut types = Vec::with_capacity(cols as usize);
            for i in 0..cols {
                let mut name = duckdb_v2_identifier_t::default();
                let mut ty = ptr::null_mut();
                check_rc(|err| {
                    duckdb_v2_schema_get_field(*schema, i, &mut name, &mut ty, err)
                })?;
                names.push(duckdb_str_to_str(name).to_owned());
                types.push(ValueWrap::try_from(ty)?.into());
            }
            Ok(Self {
                vectors: Vec::with_capacity(cols as usize),
                chunk: None,
                result,
                names: names.into(),
                types,
                chunk_size: 0,
                position: 0,
            })
        }
    }

    #[inline(always)]
    fn next_buffered_row(&mut self) -> Result<Option<RowOwned>> {
        if self.position >= self.chunk_size {
            return Ok(None);
        }
        let row = self.position;
        let values = self
            .vectors
            .iter()
            .zip(self.types.iter())
            .map(|(vec_node, ty)| vec_node.extract(row, ty))
            .collect::<Result<Vec<_>>>()?;
        self.position += 1;
        if self.position == self.chunk_size {
            self.vectors.clear();
            self.chunk = None;
            self.chunk_size = 0;
            self.position = 0;
        }
        Ok(Some(RowOwned::new(self.names.clone(), values)))
    }

    fn load_chunk(&mut self, chunk: CBox<duckdb_v2_data_chunk_handle>) -> Result<bool> {
        unsafe {
            let mut size = 0;
            check_rc(|err| duckdb_v2_data_chunk_get_size(*chunk, &mut size, err))?;
            if size == 0 {
                return Ok(false);
            }
            self.vectors.clear();
            for (col, ty) in self.types.iter().enumerate() {
                let mut vector = ptr::null_mut();
                check_rc(|err| {
                    duckdb_v2_data_chunk_get_vector(*chunk, col as idx_t, &mut vector, err)
                })?;
                self.vectors.push(VectorNode::from_vector(vector, ty)?);
            }
            self.chunk_size = size as usize;
            self.position = 0;
            self.chunk = Some(chunk);
            Ok(true)
        }
    }
}

enum ActiveResult {
    Rows(NativeRows),
    Affected {
        result: CBox<duckdb_v2_result_handle>,
        rows_changed: Option<u64>,
    },
}

pub struct Statements<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    active: Option<ActiveResult>,
    statement: Option<CBox<duckdb_v2_sql_statement_handle>>,
    iterator: Option<CBox<duckdb_v2_statement_iterator_handle>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    context: String,
    executed_prepared: bool,
    finished: bool,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Statements<'s, Q> {
    const STEP_BUDGET: usize = 64;

    pub(crate) fn new(connection: &'s mut DuckDBConnection, mut query: Q) -> Result<Self> {
        let context = format!("While running the query:\n{}", query.as_mut());
        let iterator = if let Query::Raw(raw) = query.as_mut() {
            let sql = CString::new(raw.0.as_bytes()).with_context(|| context.clone())?;
            let mut iter = CBox::new(ptr::null_mut(), |mut p| unsafe {
                duckdb_v2_statement_iterator_destroy(&mut p);
            });
            unsafe {
                check_rc(|err| {
                    duckdb_v2_parse_sql(*connection.connection, sql.as_ptr(), &mut *iter, err)
                })
                .with_context(|| context.clone())?;
            }
            Some(iter)
        } else {
            None
        };
        Ok(Self {
            active: None,
            statement: None,
            iterator,
            query,
            connection,
            context,
            executed_prepared: false,
            finished: false,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.active = None;
        self.statement = None;
        self.iterator = None;
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

    fn step_once(
        result: duckdb_v2_result_handle,
        steps: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(DUCKDB_V2_RESULT_STEP_STATUS, Option<CBox<duckdb_v2_data_chunk_handle>>)>> {
        if *steps >= Self::STEP_BUDGET {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        *steps += 1;
        let mut raw_chunk = ptr::null_mut();
        let mut status = DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
        if let Err(e) = unsafe {
            check_rc(|err| duckdb_v2_result_step(result, &mut raw_chunk, &mut status, err))
        } {
            return Poll::Ready(Err(e));
        }
        let chunk = (!raw_chunk.is_null()).then(|| {
            CBox::new(raw_chunk, |mut c| unsafe {
                duckdb_v2_data_chunk_destroy(&mut c);
            })
        });
        Poll::Ready(Ok((status, chunk)))
    }

    pub(crate) fn poll_affected(
        result: duckdb_v2_result_handle,
        rows_changed: &mut Option<u64>,
        steps: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RowsAffected>> {
        loop {
            let Poll::Ready(step) = Self::step_once(result, steps, cx) else {
                return Poll::Pending;
            };
            let (status, chunk) = step?;
            match status {
                DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING => {}
                DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CHUNK => {
                    if let (None, Some(chunk)) = (&*rows_changed, chunk) {
                        *rows_changed = Some(Self::extract_changed_rows(*chunk)?);
                    }
                }
                DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_FINISHED => {
                    return Poll::Ready(Ok(RowsAffected {
                        rows_affected: Some(rows_changed.unwrap_or(0)),
                        last_affected_id: None,
                    }));
                }
                _ => return Poll::Ready(Err(anyhow!("DuckDB query was interrupted"))),
            }
        }
    }

    fn extract_changed_rows(chunk: duckdb_v2_data_chunk_handle) -> Result<u64> {
        unsafe {
            let mut size = 0;
            check_rc(|err| duckdb_v2_data_chunk_get_size(chunk, &mut size, err))?;
            if size == 0 {
                return Ok(0);
            }
            let mut vec = ptr::null_mut();
            check_rc(|err| duckdb_v2_data_chunk_get_vector(chunk, 0, &mut vec, err))?;
            let mut vt = DUCKDB_V2_VECTOR_TYPE::DUCKDB_V2_VECTOR_TYPE_OTHER;
            check_rc(|err| duckdb_v2_vector_get_vector_type(vec, &mut vt, err))?;
            if vt != DUCKDB_V2_VECTOR_TYPE::DUCKDB_V2_VECTOR_TYPE_FLAT {
                check_rc(|err| duckdb_v2_vector_flatten(vec, err))?;
            }
            let mut view: duckdb_v2_vector_view = mem::zeroed();
            check_rc(|err| duckdb_v2_vector_get_view(vec, &mut view, err))?;
            if view.data.is_null() {
                return Ok(0);
            }
            Ok((*(view.data as *const i64)).max(0) as u64)
        }
    }

    fn classify_result(result: CBox<duckdb_v2_result_handle>) -> Result<ActiveResult> {
        unsafe {
            let mut result_type = DUCKDB_V2_RESULT_TYPE::DUCKDB_V2_RESULT_TYPE_NOTHING;
            check_rc(|err| duckdb_v2_result_get_result_type(*result, &mut result_type, err))?;
            match result_type {
                DUCKDB_V2_RESULT_TYPE::DUCKDB_V2_RESULT_TYPE_QUERY_RESULT => {
                    Ok(ActiveResult::Rows(NativeRows::new(result)?))
                }
                DUCKDB_V2_RESULT_TYPE::DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS => {
                    Ok(ActiveResult::Affected {
                        result,
                        rows_changed: None,
                    })
                }
                _ => Ok(ActiveResult::Affected {
                    result,
                    rows_changed: Some(0),
                }),
            }
        }
    }

    fn start_next_statement(&mut self) -> Result<bool> {
        if self.finished {
            return Ok(false);
        }
        unsafe {
            let mut result = CBox::new(ptr::null_mut(), |mut r| {
                duckdb_v2_result_destroy(&mut r);
            });
            if let Some(iterator) = &self.iterator {
                self.statement = None;
                let mut stmt = CBox::new(ptr::null_mut(), |mut s| {
                    duckdb_v2_sql_statement_destroy(&mut s);
                });
                check_rc(|err| duckdb_v2_statement_iterator_next(**iterator, &mut *stmt, err))?;
                if stmt.is_null() {
                    self.finish()?;
                    return Ok(false);
                }
                check_rc(|err| {
                    duckdb_v2_statement_execute(
                        *self.connection.connection,
                        *stmt,
                        ptr::null(),
                        ptr::null(),
                        0,
                        &mut *result,
                        err,
                    )
                })?;
                self.statement = Some(stmt);
            } else {
                if self.executed_prepared {
                    self.finish()?;
                    return Ok(false);
                }
                self.executed_prepared = true;
                let factory = self.connection.factory;
                let Query::Prepared(prepared) = self.query.as_mut() else {
                    return Err(anyhow!("Expected a prepared query"));
                };
                if prepared.is_empty() {
                    self.finish()?;
                    return Ok(false);
                }
                let params = prepared.take_params(factory)?;
                let raw_params = params.iter().map(|v| **v).collect::<Vec<_>>();
                check_rc(|err| {
                    duckdb_v2_prepared_statement_execute(
                        prepared.statement(),
                        ptr::null(),
                        if raw_params.is_empty() {
                            ptr::null()
                        } else {
                            raw_params.as_ptr()
                        },
                        raw_params.len() as idx_t,
                        &mut *result,
                        err,
                    )
                })?;
            }
            self.active = Some(Self::classify_result(result)?);
            Ok(true)
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for Statements<'_, Q> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            log::error!("While dropping DuckDB execution: {error:#}");
        }
    }
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Stream for Statements<'s, Q> {
    type Item = Result<QueryResult<RowOwned>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut steps = 0;
        loop {
            if this.finished {
                return Poll::Ready(None);
            }
            match &mut this.active {
                Some(ActiveResult::Rows(rows)) => {
                    match rows.next_buffered_row() {
                        Ok(Some(row)) => return Poll::Ready(Some(Ok(QueryResult::Row(row)))),
                        Ok(None) => {}
                        Err(e) => return Poll::Ready(Some(Err(this.fail(e)))),
                    }
                    let Poll::Ready(step) = Self::step_once(*rows.result, &mut steps, cx) else {
                        return Poll::Pending;
                    };
                    match step {
                        Ok((
                            DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING,
                            _,
                        )) => {}
                        Ok((
                            DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CHUNK,
                            Some(chunk),
                        )) => {
                            if let Err(e) = rows.load_chunk(chunk) {
                                return Poll::Ready(Some(Err(this.fail(e))));
                            }
                        }
                        Ok((
                            DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_FINISHED,
                            _,
                        )) => {
                            this.active = None;
                            this.statement = None;
                        }
                        Ok(_) => {
                            return Poll::Ready(Some(Err(
                                this.fail(anyhow!("DuckDB query was interrupted"))
                            )));
                        }
                        Err(e) => return Poll::Ready(Some(Err(this.fail(e)))),
                    }
                }
                Some(ActiveResult::Affected {
                    result,
                    rows_changed,
                }) => {
                    let Poll::Ready(res) =
                        Self::poll_affected(**result, rows_changed, &mut steps, cx)
                    else {
                        return Poll::Pending;
                    };
                    match res {
                        Ok(affected) => {
                            this.active = None;
                            this.statement = None;
                            return Poll::Ready(Some(Ok(QueryResult::Affected(affected))));
                        }
                        Err(e) => return Poll::Ready(Some(Err(this.fail(e)))),
                    }
                }
                None => match this.start_next_statement() {
                    Ok(true) => {}
                    Ok(false) => return Poll::Ready(None),
                    Err(e) => return Poll::Ready(Some(Err(this.fail(e)))),
                },
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.active {
            Some(ActiveResult::Rows(rows)) if rows.position < rows.chunk_size => {
                (rows.chunk_size - rows.position, None)
            }
            _ => (0, None),
        }
    }
}
