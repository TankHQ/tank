use crate::{
    ChdbDriver, ChdbPrepared, ChdbSqlWriter, ChdbTransaction,
    streaming::ChdbStream,
    value_wrap::{JsonRowParser, build_chdb_path},
};
use anyhow::anyhow;
use async_stream::try_stream;
use chdb_rust::{connection::Connection as ChConnection, format::OutputFormat};
use flume::Sender;
use std::{
    borrow::Cow,
    fmt, mem,
    sync::{Arc, Mutex},
};
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, RawQuery, Result,
    RowsAffected, send_value, stream::Stream,
};
use tokio::task::spawn_blocking;

/// chDB connection.
pub struct ChdbConnection {
    pub(crate) connection: Arc<Mutex<ChConnection>>,
}

impl fmt::Debug for ChdbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChdbConnection").finish()
    }
}

impl Executor for ChdbConnection {
    type Driver = ChdbDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(&mut self, sql: String) -> Result<Query<ChdbDriver>> {
        Ok(Query::Prepared(ChdbPrepared::new(sql)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ChdbDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        let connection = Arc::clone(&self.connection);
        let (tx, rx) = flume::bounded::<Result<QueryResult>>(16);
        let mut owned = mem::take(query.as_mut());
        let join = spawn_blocking(move || {
            match &mut owned {
                Query::Raw(RawQuery(sql)) => Self::do_run(connection, sql, tx),
                Query::Prepared(prepared) => match prepared.build_sql(&ChdbSqlWriter::chdb()) {
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
            let mut query_error = None;
            while let Ok(result) = rx.recv_async().await {
                match result {
                    Ok(result) => yield result,
                    Err(error) => {
                        let error = error.context(context.clone());
                        log::error!("{error:#}");
                        query_error = Some(error);
                        break;
                    }
                }
            }
            *query.as_mut() = mem::take(&mut join.await?);
            query.as_mut().clear_bindings().context(context.clone())?;
            if let Some(error) = query_error {
                Err(error)?;
            }
        }
    }
}

impl ChdbConnection {
    fn do_run(connection: Arc<Mutex<ChConnection>>, sql: &str, tx: Sender<Result<QueryResult>>) {
        let result = (|| -> Result<usize> {
            let connection = connection
                .lock()
                .map_err(|e| anyhow!("chDB connection lock poisoned: {e}"))?;
            if !returns_rows(sql) {
                connection
                    .query(sql, OutputFormat::Null)
                    .map_err(|e| anyhow!("chDB query failed: {e}"))?;
                return Ok(0);
            }
            let mut stream = ChdbStream::start(&connection, sql)?;
            let mut parser = JsonRowParser::new();
            while let Some(chunk) = stream.next()? {
                parser.push(chunk.data(), |row| send_value!(tx, Ok(row)))?;
            }
            parser.finish(|row| send_value!(tx, Ok(row)))?;
            Ok(parser.rows())
        })();

        match result {
            Ok(0) => {
                send_value!(
                    tx,
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: None,
                        last_affected_id: None,
                    }))
                );
            }
            Err(error) => send_value!(tx, Err(error)),
            Ok(_) => {}
        }
    }
}

fn returns_rows(sql: &str) -> bool {
    let keyword = sql.trim_start().split_ascii_whitespace().next();
    matches!(
        keyword,
        Some(
            "SELECT"
                | "select"
                | "WITH"
                | "with"
                | "SHOW"
                | "show"
                | "DESCRIBE"
                | "describe"
                | "DESC"
                | "desc"
                | "EXPLAIN"
                | "explain"
        )
    )
}

impl Connection for ChdbConnection {
    async fn connect(driver: &ChdbDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to chDB";
        let url = Self::sanitize_url(driver, url).map_err(|e| {
            log::error!("{e:#}");
            e
        })?;
        let path = build_chdb_path(&url).map(Cow::into_owned);
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

    fn begin(&mut self) -> impl Future<Output = Result<ChdbTransaction<'_>>> + Send {
        ChdbTransaction::new(self)
    }
}
