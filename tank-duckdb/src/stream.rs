use crate::{
    DuckDBConnection, DuckDBDriver, ensure_rc,
    row::{Batch, DuckDBRow},
    util::CBox,
};
use libduckdb_sys::*;
use std::{
    ffi::{CStr, CString},
    hint, mem,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tank_core::{
    Error, ErrorContext, Query, QueryResult, Result, RowsAffected, error_message_from_ptr,
    log_error, stream::Stream,
};

pub struct DuckDBStream<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    batch: Option<Arc<Batch>>,
    row: usize,
    size: usize,
    result: Option<CBox<duckdb_result>>,
    extracted: Option<CBox<duckdb_extracted_statements>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    count: u64,
    index: u64,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> DuckDBStream<'s, Q> {
    #[inline(always)]
    pub(crate) fn new(connection: &'s mut DuckDBConnection, query: Q) -> Self {
        Self {
            batch: None,
            row: 0,
            size: 0,
            result: None,
            extracted: None,
            query,
            connection,
            count: 1,
            index: 0,
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.row = 0;
        self.size = 0;
        self.batch = None;
        self.result = None;
        self.extracted = None;
        if self.count != 0 {
            self.count = 0;
            self.index = 0;
            self.query.as_mut().clear_bindings()?;
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn fail(&mut self, error: Error) -> Error {
        let context = format!("While running the query:\n{}", self.query.as_mut());
        let _ = self
            .finish()
            .context("While cleaning up DuckDB execution")
            .inspect_err(log_error);
        error.context(context)
    }

    #[cold]
    #[inline(never)]
    fn advance(&mut self) -> Result<Option<RowsAffected>> {
        unsafe {
            loop {
                if let Some(result) = &mut self.result {
                    let chunk = CBox(duckdb_fetch_chunk(**result));
                    if chunk.is_null() {
                        let error = duckdb_result_error(result.deref_mut());
                        if !error.is_null() {
                            hint::cold_path();
                            return Err(Error::msg(
                                CStr::from_ptr(error).to_string_lossy().into_owned(),
                            ));
                        }
                        self.row = 0;
                        self.size = 0;
                        self.result = None;
                        continue;
                    }
                    let size = duckdb_data_chunk_get_size(*chunk) as usize;
                    if size == 0 {
                        continue;
                    }
                    // Rows of the previous chunk may still be alive, keep it in their batch.
                    let batch = match &self.batch {
                        Some(batch) if self.size != 0 => batch.next(chunk),
                        _ => Batch::new(&mut **result, chunk),
                    };
                    self.batch = Some(Arc::new(batch));
                    self.row = 0;
                    self.size = size;
                    return Ok(None);
                }
                if self.count != 0
                    && self.extracted.is_none()
                    && let Query::Raw(raw) = self.query.as_mut()
                {
                    let sql = CString::new(raw.0.as_bytes())?;
                    let mut extracted = CBox::null();
                    self.count = duckdb_extract_statements(
                        *self.connection.connection,
                        sql.as_ptr(),
                        &mut *extracted,
                    );
                    let error = duckdb_extract_statements_error(*extracted);
                    if !error.is_null() {
                        hint::cold_path();
                        return Err(Error::msg(
                            CStr::from_ptr(error).to_string_lossy().into_owned(),
                        ));
                    }
                    self.extracted = Some(extracted);
                }
                if self.index == self.count {
                    self.finish()?;
                    return Ok(None);
                }
                let mut extracted_stmt = CBox::null();
                let statement = if let Some(extracted) = &self.extracted {
                    ensure_rc!(
                        duckdb_prepare_extracted_statement(
                            *self.connection.connection,
                            **extracted,
                            self.index,
                            &mut *extracted_stmt,
                        ),
                        Error::msg(
                            error_message_from_ptr(&duckdb_prepare_error(*extracted_stmt))
                                .into_owned(),
                        )
                    );
                    *extracted_stmt
                } else {
                    match self.query.as_mut() {
                        Query::Prepared(query) => *query.statement,
                        Query::Raw(_) => unreachable!(),
                    }
                };
                self.index += 1;
                let mut result = CBox(mem::zeroed());
                ensure_rc!(
                    duckdb_execute_prepared_streaming(statement, &mut *result),
                    Error::msg(
                        error_message_from_ptr(&duckdb_result_error(&mut *result)).into_owned(),
                    )
                );
                if duckdb_result_return_type(*result)
                    != duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT
                {
                    return Ok(Some(RowsAffected {
                        rows_affected: Some(duckdb_rows_changed(&mut *result)),
                        ..Default::default()
                    }));
                }
                self.result = Some(result);
            }
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Stream for DuckDBStream<'_, Q> {
    type Item = Result<QueryResult<DuckDBRow>>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            if this.row < this.size {
                let row = this.row;
                this.row = row + 1;
                return Poll::Ready(Some(Ok(QueryResult::Row(DuckDBRow {
                    batch: unsafe { this.batch.as_ref().unwrap_unchecked() }.clone(),
                    row,
                }))));
            }
            match this.advance() {
                Ok(Some(affected)) => {
                    return Poll::Ready(Some(Ok(QueryResult::Affected(affected))));
                }
                Ok(None) if this.row < this.size => {}
                Ok(None) => return Poll::Ready(None),
                Err(error) => return Poll::Ready(Some(Err(this.fail(error)))),
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.size - self.row;
        (remaining, (remaining > 0).then_some(remaining))
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for DuckDBStream<'_, Q> {
    fn drop(&mut self) {
        let _ = self
            .finish()
            .context("While dropping DuckDB execution")
            .inspect_err(log_error);
    }
}
