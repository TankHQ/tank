use crate::{
    ChdbDriver, ChdbPrepared, ChdbSqlWriter, ChdbTransaction,
    value_wrap::{build_chdb_path, json_compact_to_results},
};
use anyhow::anyhow;
use async_stream::try_stream;
use chdb_rust::{connection::Connection as ChConnection, format::OutputFormat};
use std::{borrow::Cow, fmt};
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, RawQuery, Result,
    RowsAffected, stream::Stream,
};

/// chDB connection.
pub struct ChdbConnection {
    pub(crate) connection: ChConnection,
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

            let result = self
                .connection
                .query(&sql, OutputFormat::JSONCompactEachRowWithNamesAndTypes)
                .map_err(|e| anyhow!("chDB query failed: {e}"))
                .context(context.clone())?;

            let rows = json_compact_to_results(result.data_ref()).context(context.clone())?;
            if rows.is_empty() {
                yield QueryResult::Affected(RowsAffected {
                    rows_affected: None,
                    last_affected_id: None,
                });
            } else {
                for row in rows {
                    yield row;
                }
            }
        }
        .map_err(move |e| {
            log::error!("{e:#}");
            e
        })
    }
}

impl Connection for ChdbConnection {
    async fn connect(driver: &ChdbDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to chDB";
        let url = Self::sanitize_url(driver, url).map_err(|e| {
            log::error!("{e:#}");
            e
        })?;
        let connection = match build_chdb_path(&url) {
            Some(path) => ChConnection::open_with_path(&path)
                .map_err(|e| anyhow!("Cannot open chDB at '{}': {e}", path))
                .context(context)
                .map_err(|e| {
                    log::error!("{e:#}");
                    e
                })?,
            None => ChConnection::open_in_memory()
                .map_err(|e| anyhow!("Cannot open in-memory chDB: {e}"))
                .context(context)
                .map_err(|e| {
                    log::error!("{e:#}");
                    e
                })?,
        };
        for sql in &[
            "SET allow_experimental_lightweight_delete=1",
            "SET join_use_nulls=1",
            "SET final=1",
        ] {
            connection
                .query(sql, OutputFormat::Null)
                .map_err(|e| anyhow!("Failed to apply session setting '{sql}': {e}"))
                .context(context)
                .map_err(|e| {
                    log::error!("{e:#}");
                    e
                })?;
        }
        Ok(Self { connection })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ChdbTransaction<'_>>> + Send {
        ChdbTransaction::new(self)
    }
}
