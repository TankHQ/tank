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
    sync::{Arc, Mutex},
};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, OwnedResults, Query, QueryResult,
    QueryResultOwned, RawQuery, Result, Results, describe_path, send_value,
};
use tokio::task::spawn_blocking;

/// Wrapper around chdb connection.
/// Provides helpers to execute queries and extract results into `tank_core` types.
#[derive(Debug)]
pub struct ChDBConnection {
    pub(crate) connection: Arc<Mutex<ChConnection>>,
}

impl ChDBConnection {
    fn do_run(
        connection: Arc<Mutex<ChConnection>>,
        sql: &str,
        sender: Sender<Result<QueryResultOwned>>,
    ) {
        if let Err(e) = Self::extract_result(connection, sql, &sender) {
            send_value!(sender, Err(e));
        }
    }

    fn extract_result(
        connection: Arc<Mutex<ChConnection>>,
        sql: &str,
        sender: &Sender<Result<QueryResultOwned>>,
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
            let mut stream = ChDBStream::start(&connection, sql)?;
            let mut parser = JsonRowParser::new();
            while let Some(chunk) = stream.next()? {
                parser.push(chunk.data(), |row| send_value!(sender, Ok(row)))?;
            }
            parser.finish(|row| send_value!(sender, Ok(row)))?;
        } else {
            connection
                .query(sql, OutputFormat::Null)
                .map_err(|e| anyhow!("chDB query failed: {e:#}"))?;
            send_value!(sender, Ok(QueryResult::Affected(Default::default())));
        }
        Ok(())
    }
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to chDB {}",
                describe_path::<ChDBDriver>(&url)
            )
        };
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
                    ChConnection::open(&[&arg])
                        .map_err(|e| anyhow!("Cannot open chDB at '{path}': {e}"))?
                }
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
        .map_err(Error::new)
        .flatten()
        .with_context(make_context)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ChDBTransaction<'_>>> + Send {
        ChDBTransaction::new(self)
    }
}

impl Executor for ChDBConnection {
    type Driver = ChDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(
        &mut self,
        RawQuery { sql, params }: RawQuery,
    ) -> Result<Query<ChDBDriver>> {
        Ok(Query::Prepared(ChDBPrepared::new(sql, params)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ChDBDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl Results>> + Send {
        async move {
            let mut query = query.as_query();
            let context = format!("While running the query:\n{}", query.as_mut());
            let connection = Arc::clone(&self.connection);
            let mut owned = mem::take(query.as_mut());
            let (sender, receiver) = flume::unbounded::<Result<QueryResultOwned>>();
            let join = spawn_blocking(move || {
                match &mut owned {
                    Query::Raw(RawQuery { sql, .. }) => Self::do_run(connection, sql, sender),
                    Query::Prepared(prepared) => match prepared.build_sql(&ChDBSqlWriter::chdb()) {
                        Ok((directive, sql)) => {
                            prepared.take_params();
                            let apply = match directive {
                                Some(directive) => connection
                                    .lock()
                                    .map_err(|e| anyhow!("chDB connection lock poisoned: {e:#?}"))
                                    .and_then(|lock| {
                                        lock.query(&directive, OutputFormat::Null)
                                            .map(|_| ())
                                            .map_err(|e| anyhow!("chDB query failed: {e:#}"))
                                    }),
                                None => Ok(()),
                            };
                            match apply {
                                Ok(()) => Self::do_run(connection, &sql, sender),
                                Err(error) => send_value!(sender, Err(error)),
                            }
                        }
                        Err(error) => send_value!(sender, Err(error)),
                    },
                }
                owned
            });
            let stream = try_stream! {
                while let Ok(result) = receiver.recv_async().await {
                    yield result.map_err(|e| {
                        let error = e.context(context.clone());
                        log::error!("{error:#}");
                        error
                    })?;
                }
                *query.as_mut() = mem::take(&mut join.await?);
                query.as_mut().clear_bindings().context(context)?;
            };
            OwnedResults::from_query_result_stream(stream).await
        }
    }
}
