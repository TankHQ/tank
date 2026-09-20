use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction, JsonRowParser, streaming::ChDBStream,
};
use anyhow::anyhow;
use async_stream::try_stream;
use chdb_rust::{connection::Connection as ChConnection, format::OutputFormat};
use flume::Sender;
use std::{
    borrow::Cow,
    mem,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, RawQuery, Result, send_value,
    stream::Stream,
};
use tokio::task::spawn_blocking;

/// Wrapper around chdb connection.
/// Provides helpers to execute queries and extract results into `tank_core` types.
#[derive(Debug)]
pub struct ChDBConnection {
    pub(crate) connection: Arc<Mutex<ChConnection>>,
}

/// The query currently executing (`Some`) or idle (`None`), plus when it started.
///
/// Updated by the blocking worker in [`ChDBConnection::do_run`]. A background
/// watchdog thread reads it to report queries that have been stuck for a while,
/// which is how we localise a hard hang in the native chDB library.
static IN_FLIGHT: OnceLock<Mutex<Option<(String, Instant)>>> = OnceLock::new();

fn in_flight() -> &'static Mutex<Option<(String, Instant)>> {
    IN_FLIGHT.get_or_init(|| Mutex::new(None))
}

fn start_watchdog() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        std::thread::spawn(|| loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            if let Ok(guard) = in_flight().lock()
                && let Some((sql, started)) = guard.as_ref()
                && started.elapsed() >= std::time::Duration::from_secs(15)
            {
                eprintln!(
                    "[tank] chDB WATCHDOG: query has been running for {:?} and has not \
                     returned; the worker thread is almost certainly blocked inside native \
                     chDB. Query:\n{sql}",
                    started.elapsed()
                );
            }
        });
    });
}

impl ChDBConnection {
    fn do_run(connection: Arc<Mutex<ChConnection>>, sql: &str, tx: Sender<Result<QueryResult>>) {
        start_watchdog();
        eprintln!("[tank] chDB do_run START (thread {:?}): {sql}", std::thread::current().id());
        if let Ok(mut guard) = in_flight().lock() {
            *guard = Some((sql.to_owned(), Instant::now()));
        }
        let result = Self::extract_result(connection, sql, tx.clone());
        if let Ok(mut guard) = in_flight().lock() {
            *guard = None;
        }
        if let Err(e) = result {
            send_value!(tx, Err(e));
        }
        eprintln!("[tank] chDB do_run END (thread {:?}): {sql}", std::thread::current().id());
    }

    fn extract_result(
        connection: Arc<Mutex<ChConnection>>,
        sql: &str,
        tx: Sender<Result<QueryResult>>,
    ) -> Result<()> {
        let connection = connection
            .lock()
            .map_err(|e| anyhow!("chDB connection lock poisoned: {e:#?}"))?;
        let returns_rows = sql
            .trim_start()
            .split_ascii_whitespace()
            .next()
            .is_some_and(|keyword| {
                ["SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"]
                    .iter()
                    .any(|k| keyword.eq_ignore_ascii_case(k))
            });
        if returns_rows {
            let started = std::time::Instant::now();
            log::info!("chDB stream query start: {sql}");
            eprintln!("[tank] chDB stream start: {sql}");
            let mut stream = ChDBStream::start(&connection, sql)?;
            let mut parser = JsonRowParser::new();
            let mut chunk_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            while let Some(chunk) = stream.next()? {
                chunk_count += 1;
                total_bytes += chunk.data().len() as u64;
                if chunk_count % 100_000 == 0 {
                    eprintln!(
                        "[tank] chDB stream STILL RUNNING after {chunk_count} chunks \
                         ({total_bytes} bytes, {:?}): {sql}",
                        started.elapsed()
                    );
                }
                log::debug!(
                    "chDB stream query chunk #{chunk_count} ({} bytes): {sql}",
                    chunk.data().len()
                );
                parser.push(chunk.data(), |row| send_value!(tx, Ok(row)))?;
            }
            parser.finish(|row| send_value!(tx, Ok(row)))?;
            log::info!("chDB stream query done ({chunk_count} chunks): {sql}");
            eprintln!(
                "[tank] chDB stream done in {:?} ({chunk_count} chunks, {total_bytes} bytes): {sql}",
                started.elapsed()
            );
        } else {
            connection
                .query(sql, OutputFormat::Null)
                .map_err(|e| anyhow!("chDB query failed: {e:#}"))?;
            send_value!(tx, Ok(QueryResult::Affected(Default::default())));
        }
        Ok(())
    }
}

impl Executor for ChDBConnection {
    type Driver = ChDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(&mut self, sql: String) -> Result<Query<ChDBDriver>> {
        Ok(Query::Prepared(ChDBPrepared::new(sql)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ChDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        let connection = Arc::clone(&self.connection);
        let mut owned = mem::take(query.as_mut());
        let (tx, rx) = flume::unbounded::<Result<QueryResult>>();
        let join = spawn_blocking(move || {
            match &mut owned {
                Query::Raw(RawQuery(sql)) => Self::do_run(connection, sql, tx),
                Query::Prepared(prepared) => match prepared.build_sql(&ChDBSqlWriter::chdb()) {
                    Ok(sql) => {
                        prepared.take_params();
                        Self::do_run(connection, &sql, tx);
                    }
                    Err(error) => send_value!(tx, Err(error)),
                },
            }
            owned
        });
        try_stream! {
            while let Ok(result) = rx.recv_async().await {
                yield result.map_err(|e| {
                    let error = e.context(context.clone());
                    log::error!("{error:#}");
                    error
                })?;
            }
            log::debug!("chDB run waiting for the blocking task to finish");
            *query.as_mut() = mem::take(&mut join.await?);
            query.as_mut().clear_bindings().context(context)?;
        }
    }
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to chDB";
        let url = Self::sanitize_url(driver, url).context(context)?;
        eprintln!("[tank] chDB connect: url={url}");
        let path: Option<Cow<'static, str>> = url
            .query_pairs()
            .find_map(|(k, v)| (k.eq_ignore_ascii_case("path") && !v.is_empty()).then_some(v))
            .map(|v| Cow::Owned(v.to_string()))
            .or_else(|| {
                let raw = url.path().trim();
                (!raw.is_empty() && raw != "/")
                    .then(|| Cow::Owned(raw.trim_start_matches('/').to_string()))
            });
        let connection = spawn_blocking(move || -> Result<ChConnection> {
            let connection = match path {
                Some(path) => {
                    let arg = format!("--path={path}");
                    eprintln!("[tank] chDB opening at '{path}'");
                    ChConnection::open(&[&arg])
                        .map_err(|e| anyhow!("Cannot open chDB at '{path}': {e}"))?
                }
                None => {
                    eprintln!("[tank] chDB opening in-memory");
                    ChConnection::open_in_memory()
                        .map_err(|e| anyhow!("Cannot open in-memory chDB: {e}"))?
                }
            };
            for sql in &[
                "SET allow_experimental_lightweight_delete=1",
                "SET join_use_nulls=1",
                "SET final=1",
                "SET output_format_json_quote_decimals=1",
            ] {
                connection
                    .query(sql, OutputFormat::Null)
                    .map_err(|e| anyhow!("Failed to apply session setting '{sql}': {e}"))?;
            }
            Ok(connection)
        })
        .await
        .context(context)?
        .context(context)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ChDBTransaction<'_>>> + Send {
        ChDBTransaction::new(self)
    }
}
