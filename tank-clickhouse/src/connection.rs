use crate::{
    ClickHouseDriver, ClickHousePrepared, ClickHouseSqlWriter, ClickHouseTransaction,
    value_wrap::kl_to_tank,
};
use anyhow::anyhow;
use async_stream::try_stream;
use futures::{StreamExt, TryStreamExt};
use klickhouse::{Client, ClientOptions};
use std::{borrow::Cow, fmt, mem, sync::Arc};
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
        let mut owned = mem::take(query.as_mut());

        try_stream! {
            let mut query_error = None;
            let sql = match &mut owned {
                Query::Raw(RawQuery(sql)) => Cow::Borrowed(sql.as_str()),
                Query::Prepared(prepared) => {
                    let writer = ClickHouseSqlWriter::new();
                    match prepared.build_sql(&writer) {
                        Ok(sql) => {
                            prepared.take_params();
                            Cow::Owned(sql)
                        }
                        Err(error) => {
                            query_error = Some(error.context(context.clone()));
                            Cow::Borrowed("")
                        }
                    }
                }
            };

            let mut kl_stream = if query_error.is_none() {
                match client.query_raw(sql.as_ref()).await {
                    Ok(stream) => Some(stream),
                    Err(error) => {
                        query_error = Some(
                            anyhow!("ClickHouse query failed: {error}").context(context.clone()),
                        );
                        None
                    }
                }
            } else {
                None
            };

            let mut got_rows = false;

            if let Some(stream) = kl_stream.as_mut() {
                while let Some(block_result) = stream.next().await {
                    let block = match block_result {
                        Ok(block) => block,
                        Err(error) => {
                            query_error = Some(
                                anyhow!("ClickHouse stream error: {error}")
                                    .context(context.clone()),
                            );
                            break;
                        }
                    };

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
                    let columns: Vec<&Vec<klickhouse::Value>> =
                        block.column_data.values().collect();

                    for row_idx in 0..block.rows as usize {
                        got_rows = true;
                        let values: Result<Vec<tank_core::Value>> = (0..col_count)
                            .map(|col_idx| {
                                kl_to_tank(types[col_idx], columns[col_idx][row_idx].clone())
                                    .map_err(|e| e.context(context.clone()))
                            })
                            .collect();
                        match values {
                            Ok(values) => {
                                yield QueryResult::Row(Row::new(names.clone(), values.into()))
                            }
                            Err(error) => {
                                query_error = Some(error);
                                break;
                            }
                        }
                    }
                    if query_error.is_some() {
                        break;
                    }
                }
            }

            if query_error.is_none() && !got_rows {
                yield QueryResult::Affected(RowsAffected {
                    rows_affected: None,
                    last_affected_id: None,
                });
            }

            *query.as_mut() = owned;
            query.as_mut().clear_bindings().context(context.clone())?;
            if let Some(error) = query_error {
                Err(error)?;
            }
        }
        .map_err(move |e| {
            log::error!("{e:#}");
            e
        })
    }
}

impl Connection for ClickHouseConnection {
    async fn connect(driver: &ClickHouseDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to ClickHouse";
        let url = Self::sanitize_url(driver, url)
            .context(context)
            .map_err(|e| {
                log::error!("{e:#}");
                e
            })?;
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
            .map_err(|e| anyhow!("Cannot connect to ClickHouse at {addr}: {e}").context(context))
            .map_err(|e| {
                log::error!("{e:#}");
                e
            })?;

        for sql in &[
            "SET allow_experimental_lightweight_delete=1",
            "SET join_use_nulls=1",
            "SET final=1",
        ] {
            client
                .execute(*sql)
                .await
                .map_err(|e| {
                    anyhow!("Failed to apply session setting '{sql}': {e}").context(context)
                })
                .map_err(|e| {
                    log::error!("{e:#}");
                    e
                })?;
        }

        Ok(ClickHouseConnection { client })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ClickHouseTransaction<'_>>> + Send {
        ClickHouseTransaction::new(self)
    }
}
