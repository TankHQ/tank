use crate::{ChDBConnection, ChDBDriver, ChDBRow};
use anyhow::anyhow;
use chdb_rust::{
    ArrowQueryStream, ArrowReader,
    admin::QueryClass,
    arrow::{array::RecordBatch, datatypes::Schema},
    format::OutputFormat,
};
use std::{
    hint,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tank_core::{
    Error, Query, QueryResult, RawQuery, Result, RowLabels, RowsAffected, stream::Stream,
};

enum ArrowSource<'s> {
    None,
    Stream(ArrowQueryStream<'s>),
    Reader(ArrowReader),
}

/// Lazily runs a chDB query and streams Arrow `RecordBatch` rows or effects.
pub(crate) struct ChDBStream<'s, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    connection: Option<&'s mut ChDBConnection>,
    query: Q,
    source: ArrowSource<'s>,
    batch: Option<Arc<(RecordBatch, RowLabels)>>,
    labels: RowLabels,
    row: usize,
    size: usize,
    remaining: u16,
    started: bool,
    done: bool,
}

impl<'s, Q> ChDBStream<'s, Q>
where
    Q: AsMut<Query<ChDBDriver>>,
{
    const BATCH_SIZE: u16 = 256;

    #[inline(always)]
    pub(crate) fn new(connection: &'s mut ChDBConnection, query: Q) -> Self {
        Self {
            connection: Some(connection),
            query,
            source: ArrowSource::None,
            batch: None,
            labels: RowLabels::default(),
            row: 0,
            size: 0,
            remaining: Self::BATCH_SIZE,
            started: false,
            done: false,
        }
    }

    fn finish(&mut self) {
        self.source = ArrowSource::None;
        self.batch = None;
        self.row = 0;
        self.size = 0;
        self.done = true;
        if self.started {
            self.started = false;
            let _ = self.query.as_mut().clear_bindings();
        }
    }

    fn update_labels(&mut self, schema: &Schema) {
        let fields = schema.fields();
        if self.labels.len() == fields.len()
            && self.labels.iter().zip(fields).all(|(prev, f)| {
                let raw = f.name().as_str();
                let clean = raw
                    .rsplit('.')
                    .next()
                    .unwrap_or(raw)
                    .trim_matches('`')
                    .trim_matches('"');
                prev == clean
            })
        {
            return;
        }
        self.labels = fields
            .iter()
            .map(|f| {
                let raw = f.name().as_str();
                raw.rsplit('.')
                    .next()
                    .unwrap_or(raw)
                    .trim_matches('`')
                    .trim_matches('"')
                    .to_owned()
            })
            .collect();
    }

    fn start(&mut self) -> Result<Option<RowsAffected>> {
        self.started = true;
        let Some(connection) = self.connection.take() else {
            return Ok(None);
        };
        let (sql, params) = match self.query.as_mut() {
            Query::Raw(RawQuery(sql)) => (sql.as_str(), None),
            Query::Prepared(prepared) => {
                let params = prepared
                    .has_bound_params()
                    .then(|| prepared.build_query_params());
                (prepared.sql(), params)
            }
        };
        let mut tokens = sql
            .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ';')
            .rsplit(|c: char| c.is_ascii_whitespace());
        if matches!(
            (tokens.next(), tokens.next()),
            (Some(fmt), Some(kw)) if fmt.eq_ignore_ascii_case("null") && kw.eq_ignore_ascii_case("format")
        ) {
            return Err(anyhow!("ChDB SELECT with FORMAT Null suppresses output"));
        }

        let first = sql
            .trim_start()
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or_default();
        let mode = if first.eq_ignore_ascii_case("select") || first.eq_ignore_ascii_case("with") {
            0u8
        } else if first.eq_ignore_ascii_case("show")
            || first.eq_ignore_ascii_case("describe")
            || first.eq_ignore_ascii_case("desc")
            || first.eq_ignore_ascii_case("explain")
            || first.eq_ignore_ascii_case("exists")
            || first.eq_ignore_ascii_case("check")
        {
            1
        } else if first.eq_ignore_ascii_case("insert")
            || first.eq_ignore_ascii_case("create")
            || first.eq_ignore_ascii_case("drop")
            || first.eq_ignore_ascii_case("alter")
            || first.eq_ignore_ascii_case("delete")
            || first.eq_ignore_ascii_case("update")
            || first.eq_ignore_ascii_case("truncate")
            || first.eq_ignore_ascii_case("set")
            || first.eq_ignore_ascii_case("use")
        {
            2
        } else {
            let class = connection
                .inner
                .classify_query(sql, None)
                .map(|a| a.class)
                .unwrap_or(QueryClass::Unknown);
            if class == QueryClass::ReadOnly { 0 } else { 2 }
        };
        match mode {
            0 => {
                let stream = match params {
                    Some(params) => connection
                        .inner
                        .query_stream_arrow_with_params(sql, params, None)
                        .map_err(Error::new)?,
                    None => connection
                        .inner
                        .query_stream_arrow(sql)
                        .map_err(Error::new)?,
                };
                self.source = ArrowSource::Stream(stream);
                self.fetch_next_batch()?;
                Ok(None)
            }
            1 => {
                let reader = connection.inner.query_arrow(sql).map_err(Error::new)?;
                self.source = ArrowSource::Reader(reader);
                self.fetch_next_batch()?;
                Ok(None)
            }
            _ => {
                match params {
                    Some(params) => connection
                        .inner
                        .query_with_params(sql, OutputFormat::Null, params)
                        .map_err(Error::new)?,
                    None => connection
                        .inner
                        .query(sql, OutputFormat::Null)
                        .map_err(Error::new)?,
                };
                self.finish();
                Ok(Some(RowsAffected::default()))
            }
        }
    }

    fn fetch_next_batch(&mut self) -> Result<()> {
        loop {
            let next = match &mut self.source {
                ArrowSource::None => {
                    self.finish();
                    return Ok(());
                }
                ArrowSource::Stream(stream) => stream.next_batch().map_err(Error::new)?,
                ArrowSource::Reader(reader) => reader.next().transpose().map_err(Error::new)?,
            };
            let Some(batch) = next else {
                self.finish();
                return Ok(());
            };
            let rows = batch.num_rows();
            if rows == 0 {
                continue;
            }
            self.update_labels(batch.schema_ref());
            self.batch = Some(Arc::new((batch, self.labels.clone())));
            self.row = 0;
            self.size = rows;
            return Ok(());
        }
    }

    #[cold]
    #[inline(never)]
    fn advance(&mut self) -> Result<Option<RowsAffected>> {
        if self.done {
            self.finish();
            return Ok(None);
        }
        if !self.started {
            return self.start();
        }
        self.fetch_next_batch()?;
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
        loop {
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
                Ok(Some(affected)) => return Poll::Ready(emit_affected(affected).map(Ok)),
                Ok(None) if self.row < self.size => {}
                Ok(None) => return Poll::Ready(None),
                Err(error) => {
                    let error = fail(self, error);
                    #[cold]
                    #[inline(never)]
                    fn fail<Q: AsMut<Query<ChDBDriver>>>(
                        stream: &mut ChDBStream<'_, Q>,
                        error: Error,
                    ) -> Error {
                        let context =
                            format!("While running the query:\n{}", stream.query.as_mut());
                        stream.finish();
                        let error = error.context(context);
                        log::error!("{error:#}");
                        error
                    }
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
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
        let remaining = self.size - self.row;
        (remaining, self.done.then_some(remaining))
    }
}

// `ArrowQueryStream` holds a raw `chdb_result` pointer but exclusively borrows `&'s mut Connection`.
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
    pub(crate) fn new(connection: &'s mut ChDBConnection, query: Q) -> Self {
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
        self.0.size_hint()
    }
}
