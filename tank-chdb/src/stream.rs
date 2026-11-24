use crate::{
    ChDBConnection, ChDBDriver, ChDBRow,
    batch::ChDBBatch,
    native::{NativeConnection, NativeResult, QUERY_CLASS_READ_ONLY},
};
use anyhow::anyhow;
use std::{
    hint,
    pin::Pin,
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
    handle: Option<NativeResult>,
    chunk: Option<Arc<NativeResult>>,
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
    const STREAM_UNSUPPORTED_PREFIX: &[u8] =
        b"Code: 36. DB::Exception: Streaming query is not supported for query: ";
    const STREAM_UNSUPPORTED_SUFFIX: &[u8] = b" (BAD_ARGUMENTS)";

    #[inline(always)]
    pub(crate) const fn new(connection: &'s mut ChDBConnection, query: Q) -> Self {
        Self {
            connection,
            query,
            handle: None,
            chunk: None,
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
        if let Some(handle) = self.handle.take() {
            self.connection.native.cancel(&handle);
        }
        self.chunk = None;
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

    fn parse_next_block(&mut self, chunk: Arc<NativeResult>) -> Result<()> {
        let data_len = chunk.data()?.len();
        self.size = ChDBBatch::parse_into(&mut self.batch, Arc::clone(&chunk), &mut self.offset)?;
        self.schema_seen = true;
        self.row = 0;
        if self.offset < data_len {
            self.chunk = Some(chunk);
        } else {
            self.chunk = None;
            self.offset = 0;
        }
        Ok(())
    }

    /// Classifies common SQL prefixes without invoking ClickHouse's parser twice.
    fn is_read_only(native: &NativeConnection, sql: &str) -> bool {
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
        native.classify(sql) == QUERY_CLASS_READ_ONLY
    }

    fn start(&mut self) -> Result<Option<RowsAffected>> {
        self.started = true;
        if self.batch.is_none() {
            self.batch = self.connection.batch.take();
        }
        let has_params = match self.query.as_mut() {
            Query::Raw(_) => false,
            Query::Prepared(prepared) => {
                self.connection.prepare_params(prepared)?;
                !self.connection.params.is_empty()
            }
        };
        let sql = match self.query.as_mut() {
            Query::Raw(RawQuery(sql)) => sql.as_str(),
            Query::Prepared(_) => self.connection.draft.buffer().as_str(),
        };
        if Self::is_read_only(&self.connection.native, sql) {
            let handle = if has_params {
                self.connection
                    .native
                    .stream_with_params(sql, &self.connection.params)?
            } else {
                self.connection.native.stream(sql)?
            };
            if let Some(err) = handle.error() {
                let bytes = err.to_bytes();
                if bytes.starts_with(Self::STREAM_UNSUPPORTED_PREFIX)
                    && bytes.ends_with(Self::STREAM_UNSUPPORTED_SUFFIX)
                {
                    drop(handle);
                    return self.run_materialized(has_params);
                }
                return Err(anyhow!(
                    "ChDB streaming initialization failed: {}",
                    err.to_string_lossy()
                ));
            }
            self.handle = Some(handle);
            // Prime the first chunk before returning so early drop can cancel safely.
            self.fetch_next_chunk()?;
            return Ok(None);
        }
        self.run_materialized(has_params)
    }

    fn run_materialized(&mut self, has_params: bool) -> Result<Option<RowsAffected>> {
        let sql = match self.query.as_mut() {
            Query::Raw(RawQuery(sql)) => sql.as_str(),
            Query::Prepared(_) => self.connection.draft.buffer().as_str(),
        };
        let result = if has_params {
            self.connection
                .native
                .query_with_params(sql, "Native", &self.connection.params)?
        } else {
            self.connection.native.query(sql, "Native")?
        };
        let data = result.data()?;
        self.done = true;
        if !data.is_empty() {
            self.offset = 0;
            self.parse_next_block(Arc::new(result))?;
            Ok(None)
        } else if result.column_count() > 0 {
            self.finish();
            Ok(None)
        } else {
            self.finish();
            Ok(Some(RowsAffected::default()))
        }
    }

    #[cold]
    #[inline(never)]
    fn has_format_null(sql: &str) -> bool {
        let mut tokens = sql.trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ';')
            .rsplit(|c: char| c.is_ascii_whitespace());
        matches!(
            (tokens.next(), tokens.next()),
            (Some(fmt), Some(kw)) if fmt.eq_ignore_ascii_case("null") && kw.eq_ignore_ascii_case("format")
        )
    }

    fn fetch_next_chunk(&mut self) -> Result<()> {
        loop {
            if let Some(chunk) = self.chunk.take() {
                self.parse_next_block(chunk)?;
                if self.size > 0 {
                    return Ok(());
                }
                continue;
            }
            let Some(handle) = self.handle.as_ref() else {
                self.finish();
                return Ok(());
            };
            let chunk = self.connection.native.fetch(handle)?;
            let data = chunk.data()?;
            let rows_read = chunk.rows_read();
            let has_cols = chunk.column_count() > 0;
            if data.is_empty() {
                if rows_read > 0 {
                    return Err(anyhow!(
                        "ChDB SELECT produced rows without a Native output block"
                    ));
                }
                if !self.schema_seen {
                    let sql = match self.query.as_mut() {
                        Query::Raw(RawQuery(sql)) => sql.as_str(),
                        Query::Prepared(_) => self.connection.draft.buffer().as_str(),
                    };
                    if !has_cols || Self::has_format_null(sql) {
                        return Err(anyhow!("ChDB SELECT ended without a schema-bearing result"));
                    }
                }
                self.handle = None;
                self.finish();
                return Ok(());
            }
            self.offset = 0;
            self.parse_next_block(Arc::new(chunk))?;
            if rows_read == 0 {
                self.handle = None;
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
        if self.done && self.chunk.is_none() {
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
            (self.done && self.chunk.is_none()).then_some(remaining),
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
