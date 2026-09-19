use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction, JsonRowParser, streaming::ChDBStream,
};
use anyhow::anyhow;
use async_stream::try_stream;
use chdb_rust::{connection::Connection as ChConnection, format::OutputFormat};
use flume::Sender;
use std::{
    borrow::Cow,
    sync::{Arc, Mutex},
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

impl ChDBConnection {
    fn do_run(connection: Arc<Mutex<ChConnection>>, sql: &str, tx: Sender<Result<QueryResult>>) {
        let result = (|| -> Result<()> {
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
            if !returns_rows {
                connection
                    .query(sql, OutputFormat::Null)
                    .map_err(|e| anyhow!("chDB query failed: {e}"))?;
                send_value!(tx, Ok(QueryResult::Affected(Default::default())));
                return Ok(());
            }
            let mut stream = ChDBStream::start(&connection, sql)?;
            let mut parser = JsonRowParser::new();
            while let Some(chunk) = stream.next()? {
                parser.push(chunk.data(), |row| send_value!(tx, Ok(row)))?;
            }
            parser.finish(|row| send_value!(tx, Ok(row)))?;
            Ok(())
        })();
        if let Err(error) = result {
            send_value!(tx, Err(error));
        }
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
        let (tx, rx) = flume::unbounded::<Result<QueryResult>>();

        try_stream! {
            let sql = match query.as_mut() {
                Query::Raw(RawQuery(sql)) => sql.clone(),
                Query::Prepared(prepared) => {
                    let sql = prepared
                        .build_sql(&ChDBSqlWriter::chdb())
                        .map_err(|e| e.context(context.clone()))?;
                    prepared.take_params();
                    sql
                }
            };
            spawn_blocking(move || Self::do_run(connection, &sql, tx));

            while let Ok(result) = rx.recv_async().await {
                yield result.map_err(|e| {
                    let error = e.context(context.clone());
                    log::error!("{error:#}");
                    error
                })?;
            }
        }
    }
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to chDB";
        let url = Self::sanitize_url(driver, url).context(context)?;
        let path = url
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
                Some(path) => ChConnection::open_with_path(&path)
                    .map_err(|e| anyhow!("Cannot open chDB at '{path}': {e}"))?,
                None => ChConnection::open_in_memory()
                    .map_err(|e| anyhow!("Cannot open in-memory chDB: {e}"))?,
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
