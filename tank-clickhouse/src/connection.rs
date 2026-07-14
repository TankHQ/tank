use crate::{
    ClickHouseDriver, ClickHousePrepared, ClickHouseSqlWriter, ClickHouseTransaction,
    value_wrap::kl_to_tank,
};
use anyhow::anyhow;
use async_stream::try_stream;
use futures::StreamExt;
use klickhouse::{Client, ClientOptions};
use std::{borrow::Cow, fmt, sync::Arc};
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, RawQuery, Result, Row,
    RowsAffected, stream::Stream,
};

/// ClickHouse connection.
pub struct ClickHouseConnection {
    pub(crate) client: Client,
}

impl fmt::Debug for ClickHouseConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConnection").finish()
    }
}

impl Executor for ClickHouseConnection {
    type Driver = ClickHouseDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(&mut self, sql: String) -> Result<Query<ClickHouseDriver>> {
        Ok(Query::Prepared(ClickHousePrepared::new(sql)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ClickHouseDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        let client = self.client.clone();

        try_stream! {
            let sql = match query.as_mut() {
                Query::Raw(RawQuery(sql)) => sql.clone(),
                Query::Prepared(prepared) => {
                    let writer = ClickHouseSqlWriter::new();
                    let sql = prepared.build_sql(&writer)
                        .map_err(|e| e.context(context.clone()))?;
                    prepared.take_params();
                    sql
                }
            };

            let mut kl_stream = client
                .query_raw(sql)
                .await
                .map_err(|e| anyhow!("ClickHouse query failed: {e}").context(context.clone()))?;

            let mut got_rows = false;

            while let Some(block_result) = kl_stream.next().await {
                let block = block_result
                    .map_err(|e| anyhow!("ClickHouse stream error: {e}").context(context.clone()))?;

                if block.rows == 0 {
                    continue;
                }

                let col_count = block.column_types.len();
                let names: Arc<[String]> = block.column_types
                    .keys()
                    .map(|n| n.rsplit('.').next().unwrap_or(n).to_owned())
                    .collect::<Vec<_>>()
                    .into();
                let types: Vec<&klickhouse::Type> = block.column_types.values().collect();
                let columns: Vec<&Vec<klickhouse::Value>> = block.column_data.values().collect();

                for row_idx in 0..block.rows as usize {
                    got_rows = true;
                    let values: Result<Vec<tank_core::Value>> = (0..col_count)
                        .map(|col_idx| {
                            kl_to_tank(types[col_idx], columns[col_idx][row_idx].clone())
                                .map_err(|e| e.context(context.clone()))
                        })
                        .collect();
                    yield QueryResult::Row(Row::new(names.clone(), values?.into()));
                }
            }

            if !got_rows {
                yield QueryResult::Affected(RowsAffected {
                    rows_affected: None,
                    last_affected_id: None,
                });
            }
        }
    }
}

impl Connection for ClickHouseConnection {
    async fn connect(driver: &ClickHouseDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to ClickHouse";
        let url = Self::sanitize_url(driver, url).context(context)?;
        let host = url.host_str().unwrap_or("localhost");
        let port = url.port().unwrap_or(9000);
        let user = if url.username().is_empty() {
            "default"
        } else {
            url.username()
        };
        let password = url.password().unwrap_or("");
        let database = url.path().trim_start_matches('/');
        let database = if database.is_empty() {
            "default"
        } else {
            database
        };

        let addr = format!("{host}:{port}");
        let options = ClientOptions {
            username: user.to_string(),
            password: password.to_string(),
            default_database: database.to_string(),
            ..Default::default()
        };

        let client = Client::connect(&addr, options)
            .await
            .map_err(|e| anyhow!("Cannot connect to ClickHouse at {addr}: {e}").context(context))?;

        for sql in &[
            "SET allow_experimental_lightweight_delete=1",
            "SET join_use_nulls=1",
            "SET final=1",
        ] {
            client.execute(*sql).await.map_err(|e| {
                anyhow!("Failed to apply session setting '{sql}': {e}").context(context)
            })?;
        }

        Ok(ClickHouseConnection { client })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ClickHouseTransaction<'_>>> + Send {
        ClickHouseTransaction::new(self)
    }
}
