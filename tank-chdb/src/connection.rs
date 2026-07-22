use crate::{
    ChdbDriver, ChdbPrepared, ChdbSqlWriter, ChdbTransaction,
    value_wrap::{build_chdb_path, json_compact_to_results},
};
use anyhow::anyhow;
use async_stream::try_stream;
use chdb_rust::{connection::Connection as ChConnection, format::OutputFormat};
use flume::Sender;
use std::{
    borrow::Cow,
    fmt,
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
        let context = format!("While running the query:\n{}", query.as_mut());
        let connection = Arc::clone(&self.connection);
        let (tx, rx) = flume::unbounded::<Result<QueryResult>>();

        try_stream! {
            let sql = match query.as_mut() {
                Query::Raw(RawQuery(sql)) => sql.clone(),
                Query::Prepared(prepared) => {
                    let writer = ChdbSqlWriter::chdb();
                    let sql = prepared.build_sql(&writer).context(context.clone())?;
                    prepared.take_params();
                    sql
                }
            };

            let join = spawn_blocking(move || {
                Self::do_run(connection, sql, tx);
            });
            while let Ok(result) = rx.recv_async().await {
                yield result.map_err(|e| {
                    let error = e.context(context.clone());
                    log::error!("{error:#}");
                    error
                })?;
            }
            join.await?;
        }
    }
}

impl ChdbConnection {
    fn do_run(connection: Arc<Mutex<ChConnection>>, sql: String, tx: Sender<Result<QueryResult>>) {
        let result = (|| -> Result<Vec<QueryResult>> {
            let connection = connection
                .lock()
                .map_err(|e| anyhow!("chDB connection lock poisoned: {e}"))?;
            let result = connection
                .query(&sql, OutputFormat::JSONCompactEachRowWithNamesAndTypes)
                .map_err(|e| anyhow!("chDB query failed: {e}"))?;
            json_compact_to_results(result.data_ref())
        })();

        match result {
            Ok(rows) if rows.is_empty() => {
                send_value!(
                    tx,
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: None,
                        last_affected_id: None,
                    }))
                );
            }
            Ok(rows) => {
                for row in rows {
                    send_value!(tx, Ok(row));
                }
            }
            Err(error) => send_value!(tx, Err(error)),
        }
    }
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
