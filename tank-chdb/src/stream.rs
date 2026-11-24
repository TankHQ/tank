use crate::{
    ChDBConnection, ChDBDriver, ChDBRow,
    batch::ChDBBatch,
    ffi::{
        QUERY_CLASS_READ_ONLY, chdb_destroy_query_result, chdb_query_n, chdb_query_with_params_n,
        chdb_result_column_count, chdb_result_length, chdb_result_rows_read,
        chdb_stream_cancel_query, chdb_stream_fetch_result, chdb_stream_query_n,
        chdb_stream_query_with_params_n, query_error, result_error,
    },
};
use anyhow::anyhow;
use std::{
    ffi::{c_char, c_void},
    hint, mem,
    pin::Pin,
    ptr,
    sync::Arc,
    task::{Context, Poll},
};
use tank_core::{Error, Query, QueryResult, RawQuery, Result, RowsAffected, stream::Stream};

/// Lazily runs a chDB query and streams zero-copy `Native` columnar rows or effects.
pub(crate) struct ChDBStream<'s, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    connection: &'s mut ChDBConnection,
    query: Q,
    handle: *mut c_void,
    pending_parent: Option<Arc<ChDBBatch>>,
    offset: usize,
    batch: Option<Arc<ChDBBatch>>,
    row: usize,
    size: usize,
    remaining: u16,
    started: bool,
    schema_seen: bool,
    done: bool,
}

impl<'s, Q> ChDBStream<'s, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    const BATCH_SIZE: u16 = 256;
    const FORMAT: &str = "Native";
    const STREAM_UNSUPPORTED_PREFIX: &[u8] =
        b"Code: 36. DB::Exception: Streaming query is not supported for query: ";
    const STREAM_UNSUPPORTED_SUFFIX: &[u8] = b" (BAD_ARGUMENTS)";

    #[inline(always)]
    pub(crate) const fn new(connection: &'s mut ChDBConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            handle: ptr::null_mut(),
            pending_parent: None,
            offset: 0,
            batch: None,
            row: 0,
            size: 0,
            remaining: Self::BATCH_SIZE,
            started: false,
            schema_seen: false,
            done: false,
        }
    }

    fn finish(&mut self) {
        let handle = mem::replace(&mut self.handle, ptr::null_mut());
        if !handle.is_null() {
            unsafe {
                chdb_stream_cancel_query(self.connection.raw(), handle);
                chdb_destroy_query_result(handle);
            }
        }
        self.pending_parent = None;
        if let Some(mut batch) = self.batch.take()
            && let Some(inner) = Arc::get_mut(&mut batch)
        {
            inner.release();
            self.connection.batch = Some(batch);
        }
        self.offset = 0;
        self.row = 0;
        self.size = 0;
        self.done = true;
        if self.started {
            self.started = false;
            let _ = self.query.as_mut().clear_bindings();
        }
    }

    #[cold]
    #[inline(never)]
    fn fail(&mut self, error: Error) -> Error {
        let context = format!("While running the query:\n{}", self.query.as_mut());
        self.finish();
        let error = error.context(context);
        log::error!("{error:#}");
        error
    }

    /// Invokes a chDB FFI entry point with stack-allocated parameter pointer arrays for <= 8 params.
    fn with_params<R>(
        names: &[String],
        values: &[String],
        bound: &[bool],
        call: impl FnOnce(
            *const *const c_char,
            *const usize,
            *const *const c_char,
            *const usize,
            usize,
        ) -> R,
    ) -> R {
        const STACK: usize = 8;
        let count = bound.iter().filter(|&&b| b).count();
        if count == 0 {
            return call(ptr::null(), ptr::null(), ptr::null(), ptr::null(), 0);
        }
        if count <= STACK {
            let mut n_ptrs = [ptr::null(); STACK];
            let mut n_lens = [0usize; STACK];
            let mut v_ptrs = [ptr::null(); STACK];
            let mut v_lens = [0usize; STACK];
            let mut out = 0;
            for ((name, val), &is_bound) in names.iter().zip(values).zip(bound) {
                if is_bound {
                    n_ptrs[out] = name.as_ptr().cast();
                    n_lens[out] = name.len();
                    v_ptrs[out] = val.as_ptr().cast();
                    v_lens[out] = val.len();
                    out += 1;
                }
            }
            return call(
                n_ptrs.as_ptr(),
                n_lens.as_ptr(),
                v_ptrs.as_ptr(),
                v_lens.as_ptr(),
                out,
            );
        }
        let mut n_ptrs = Vec::with_capacity(count);
        let mut n_lens = Vec::with_capacity(count);
        let mut v_ptrs = Vec::with_capacity(count);
        let mut v_lens = Vec::with_capacity(count);
        for ((name, val), &is_bound) in names.iter().zip(values).zip(bound) {
            if is_bound {
                n_ptrs.push(name.as_ptr().cast());
                n_lens.push(name.len());
                v_ptrs.push(val.as_ptr().cast());
                v_lens.push(val.len());
            }
        }
        call(
            n_ptrs.as_ptr(),
            n_lens.as_ptr(),
            v_ptrs.as_ptr(),
            v_lens.as_ptr(),
            n_ptrs.len(),
        )
    }

    fn parse_raw_chunk(&mut self, raw: *mut c_void) -> Result<()> {
        let data_len = unsafe { chdb_result_length(raw) };
        self.offset = 0;
        self.size = ChDBBatch::parse_raw(&mut self.batch, raw, &mut self.offset)?;
        self.schema_seen = true;
        self.row = 0;
        if self.offset < data_len {
            self.pending_parent = self.batch.clone();
        } else {
            self.pending_parent = None;
            self.offset = 0;
        }
        Ok(())
    }

    /// Classifies common SQL prefixes without invoking ClickHouse's parser twice.
    fn is_read_only(connection: &ChDBConnection, sql: &str) -> bool {
        let trimmed = sql.trim_start();
        let first = trimmed
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or_default();
        if first.eq_ignore_ascii_case("select") || first.eq_ignore_ascii_case("with") {
            return true;
        }
        if first.eq_ignore_ascii_case("insert")
            || first.eq_ignore_ascii_case("create")
            || first.eq_ignore_ascii_case("drop")
            || first.eq_ignore_ascii_case("alter")
            || first.eq_ignore_ascii_case("delete")
            || first.eq_ignore_ascii_case("update")
            || first.eq_ignore_ascii_case("truncate")
            || first.eq_ignore_ascii_case("set")
            || first.eq_ignore_ascii_case("use")
        {
            return false;
        }
        connection.classify(sql) == QUERY_CLASS_READ_ONLY
    }

    fn start(&mut self) -> Result<Option<RowsAffected>> {
        self.started = true;
        if self.batch.is_none() {
            self.batch = self.connection.batch.take();
        }
        let conn = self.connection.raw();
        let (sql, names, values, bound): (&str, &[String], &[String], &[bool]) =
            match self.query.as_mut() {
                Query::Raw(RawQuery(sql)) => (sql.as_str(), &[], &[], &[]),
                Query::Prepared(prepared) => prepared.prepare_for_exec(),
            };
        if Self::is_read_only(self.connection, sql) {
            let handle = if bound.iter().any(|&b| b) {
                Self::with_params(names, values, bound, |np, nl, vp, vl, count| unsafe {
                    chdb_stream_query_with_params_n(
                        conn,
                        sql.as_ptr().cast(),
                        sql.len(),
                        Self::FORMAT.as_ptr().cast(),
                        Self::FORMAT.len(),
                        np,
                        nl,
                        vp,
                        vl,
                        count,
                    )
                })
            } else {
                unsafe {
                    chdb_stream_query_n(
                        conn,
                        sql.as_ptr().cast(),
                        sql.len(),
                        Self::FORMAT.as_ptr().cast(),
                        Self::FORMAT.len(),
                    )
                }
            };
            if handle.is_null() {
                return Err(anyhow!("ChDB returned a null stream result"));
            }
            if let Some(err) = unsafe { result_error(handle) } {
                let bytes = err.to_bytes();
                let unsupported = bytes.starts_with(Self::STREAM_UNSUPPORTED_PREFIX)
                    && bytes.ends_with(Self::STREAM_UNSUPPORTED_SUFFIX);
                if unsupported {
                    unsafe { chdb_destroy_query_result(handle) };
                    return self.run_materialized();
                }
                let error = anyhow!(
                    "ChDB streaming initialization failed: {}",
                    err.to_string_lossy()
                );
                unsafe { chdb_destroy_query_result(handle) };
                return Err(error);
            }
            self.handle = handle;
            // Prime the first chunk before returning so early drop can cancel safely.
            self.fetch_next_chunk()?;
            return Ok(None);
        }
        self.run_materialized()
    }

    fn run_materialized(&mut self) -> Result<Option<RowsAffected>> {
        let conn = self.connection.raw();
        let (sql, names, values, bound): (&str, &[String], &[String], &[bool]) =
            match self.query.as_mut() {
                Query::Raw(RawQuery(sql)) => (sql.as_str(), &[], &[], &[]),
                Query::Prepared(prepared) => prepared.prepare_for_exec(),
            };
        let result = if bound.iter().any(|&b| b) {
            Self::with_params(names, values, bound, |np, nl, vp, vl, count| unsafe {
                chdb_query_with_params_n(
                    conn,
                    sql.as_ptr().cast(),
                    sql.len(),
                    Self::FORMAT.as_ptr().cast(),
                    Self::FORMAT.len(),
                    np,
                    nl,
                    vp,
                    vl,
                    count,
                )
            })
        } else {
            unsafe {
                chdb_query_n(
                    conn,
                    sql.as_ptr().cast(),
                    sql.len(),
                    Self::FORMAT.as_ptr().cast(),
                    Self::FORMAT.len(),
                )
            }
        };
        if result.is_null() {
            return Err(anyhow!("ChDB returned a null result"));
        }
        if let Some(err) = unsafe { result_error(result) } {
            let error = query_error(err);
            unsafe { chdb_destroy_query_result(result) };
            return Err(error);
        }
        let data_len = unsafe { chdb_result_length(result) };
        let col_count = unsafe { chdb_result_column_count(result) };
        self.done = true;
        if data_len > 0 {
            self.parse_raw_chunk(result)?;
            Ok(None)
        } else {
            unsafe { chdb_destroy_query_result(result) };
            self.finish();
            if col_count > 0 {
                Ok(None)
            } else {
                Ok(Some(RowsAffected::default()))
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn has_format_null(sql: &str) -> bool {
        let mut tokens = sql
            .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ';')
            .rsplit(|c: char| c.is_ascii_whitespace());
        matches!(
            (tokens.next(), tokens.next()),
            (Some(fmt), Some(kw)) if fmt.eq_ignore_ascii_case("null") && kw.eq_ignore_ascii_case("format")
        )
    }

    fn fetch_next_chunk(&mut self) -> Result<()> {
        loop {
            if let Some(parent) = self.pending_parent.take() {
                let data_len = unsafe { chdb_result_length(parent.raw_result()) };
                self.size =
                    ChDBBatch::parse_next(&mut self.batch, Arc::clone(&parent), &mut self.offset)?;
                self.schema_seen = true;
                self.row = 0;
                if self.offset < data_len {
                    self.pending_parent = Some(parent);
                } else {
                    self.offset = 0;
                }
                if self.size > 0 {
                    return Ok(());
                }
                continue;
            }
            if self.handle.is_null() {
                self.finish();
                return Ok(());
            }
            let chunk = unsafe { chdb_stream_fetch_result(self.connection.raw(), self.handle) };
            if chunk.is_null() {
                return Err(anyhow!("ChDB returned a null stream chunk"));
            }
            if let Some(err) = unsafe { result_error(chunk) } {
                let error = query_error(err);
                unsafe { chdb_destroy_query_result(chunk) };
                return Err(error);
            }
            let data_len = unsafe { chdb_result_length(chunk) };
            let rows_read = unsafe { chdb_result_rows_read(chunk) };
            let has_cols = unsafe { chdb_result_column_count(chunk) } > 0;
            if data_len == 0 {
                unsafe { chdb_destroy_query_result(chunk) };
                if rows_read > 0 {
                    return Err(anyhow!(
                        "ChDB SELECT produced rows without a Native output block"
                    ));
                }
                if !self.schema_seen {
                    let sql = match self.query.as_mut() {
                        Query::Raw(RawQuery(sql)) => sql.as_str(),
                        Query::Prepared(prepared) => prepared.prepare_for_exec().0,
                    };
                    if !has_cols || Self::has_format_null(sql) {
                        return Err(anyhow!("ChDB SELECT ended without a schema-bearing result"));
                    }
                }
                let handle = mem::replace(&mut self.handle, ptr::null_mut());
                unsafe { chdb_destroy_query_result(handle) };
                self.finish();
                return Ok(());
            }
            self.parse_raw_chunk(chunk)?;
            if rows_read == 0 {
                let handle = mem::replace(&mut self.handle, ptr::null_mut());
                unsafe { chdb_destroy_query_result(handle) };
                self.done = true;
            }
            if self.size > 0 {
                return Ok(());
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn advance(&mut self) -> Result<Option<RowsAffected>> {
        if self.done && self.pending_parent.is_none() {
            self.finish();
            return Ok(None);
        }
        if !self.started {
            return self.start();
        }
        self.fetch_next_chunk()?;
        Ok(None)
    }

    #[inline(always)]
    fn poll_item<T: From<ChDBRow>>(
        &mut self,
        cx: &mut Context<'_>,
        emit_affected: impl FnOnce(RowsAffected) -> Option<T>,
    ) -> Poll<Option<Result<T>>> {
        if self.remaining == 0 {
            hint::cold_path();
            self.remaining = Self::BATCH_SIZE;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if self.row < self.size
            && let Some(batch) = self.batch.as_ref()
        {
            self.remaining -= 1;
            let row = self.row;
            self.row = row + 1;
            return Poll::Ready(Some(Ok(ChDBRow {
                batch: Arc::clone(batch),
                row,
            }
            .into())));
        }
        match self.advance() {
            Ok(Some(affected)) => Poll::Ready(emit_affected(affected).map(Ok)),
            Ok(None) => {
                if self.row < self.size
                    && let Some(batch) = self.batch.as_ref()
                {
                    self.remaining -= 1;
                    let row = self.row;
                    self.row = row + 1;
                    Poll::Ready(Some(Ok(ChDBRow {
                        batch: Arc::clone(batch),
                        row,
                    }
                    .into())))
                } else {
                    Poll::Ready(None)
                }
            }
            Err(error) => Poll::Ready(Some(Err(self.fail(error)))),
        }
    }

    #[inline(always)]
    fn remaining_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.size - self.row;
        (
            remaining,
            (self.done && self.pending_parent.is_none()).then_some(remaining),
        )
    }
}

impl<Q> Stream for ChDBStream<'_, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    type Item = Result<QueryResult<ChDBRow>>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        this.poll_item(cx, |affected| Some(QueryResult::Affected(affected)))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.remaining_hint()
    }
}

// `handle` is exclusively owned by this stream and serialized with `&'s mut ChDBConnection`.
unsafe impl<Q: AsMut<Query<ChDBDriver>> + Send> Send for ChDBStream<'_, Q> {}

impl<Q: AsMut<Query<ChDBDriver>>> Drop for ChDBStream<'_, Q> {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Row-only stream returned by `Executor::fetch`, avoiding `QueryResult` wrapping per row.
pub(crate) struct ChDBRowStream<'s, Q>(ChDBStream<'s, Q>)
where
    Q: AsMut<Query<ChDBDriver>>;

impl<'s, Q> ChDBRowStream<'s, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    #[inline(always)]
    pub(crate) const fn new(connection: &'s mut ChDBConnection, query: Q) -> Self {
        Self(ChDBStream::new(connection, query))
    }
}

impl<Q> Stream for ChDBRowStream<'_, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    type Item = Result<ChDBRow>;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut unsafe { self.get_unchecked_mut() }.0;
        this.poll_item(cx, |_| None)
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.remaining_hint()
    }
}
