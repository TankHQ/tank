use crate::{ValkeyDriver, ValkeyTransaction, ValueWrap};
use async_stream::try_stream;
use redis::{Client, aio::MultiplexedConnection};
use std::{borrow::Cow, future, mem, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, Result, Row,
    RowsAffected, stream::Stream, truncate_long,
};

pub struct ValkeyConnection {
    driver: ValkeyDriver,
    pub(crate) connection: MultiplexedConnection,
}

impl Connection for ValkeyConnection {
    async fn connect(driver: &ValkeyDriver, url: Cow<'static, str>) -> Result<Self>
    where
        Self: Sized,
    {
        let context = "While trying to connect to Valkey";
        let url = Self::sanitize_url(driver, url)?;
        let client = Client::open(url.as_str()).map_err(|e| Error::msg(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(Error::new)
            .context(context)?;
        Ok(Self {
            driver: *driver,
            connection,
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ValkeyTransaction<'_>>> {
        future::ready(Ok(ValkeyTransaction {
            connection: self,
            commands: Default::default(),
        }))
    }
}

impl Executor for ValkeyConnection {
    type Driver = ValkeyDriver;

    fn driver(&self) -> Self::Driver
    where
        Self: Sized,
    {
        self.driver
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ValkeyDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
                Err(Error::msg(
                    "Query is not the expected tank::Query::Prepared variant (Valkey/Redis driver uses prepared)",
                ))?;
                return;
            };
            if prepared.is_empty() {
                return;
            }
            let pipeline = prepared.make_pipeline();
            let context = || {
                format!(
                    "While executing the query: {}",
                    truncate_long!(format!("{pipeline:?}"))
                )
            };
            let raw_result = pipeline
                .query_async::<redis::Value>(&mut self.connection)
                .await
                .map_err(Error::new)
                .with_context(context)?;
            let results = match raw_result {
                redis::Value::Array(arr) => arr,
                redis::Value::Nil => vec![],
                redis::Value::ServerError(err) => {
                    Err(Error::msg(format!("Valkey/Redis server error: {err}")))
                        .with_context(context)?;
                    return;
                }
                other => {
                    Err(Error::msg(format!(
                        "Unexpected top-level pipeline response: {:?}",
                        other
                    )))
                    .with_context(context)?;
                    return;
                }
            };
            if prepared.columns.is_empty() {
                yield QueryResult::Affected(RowsAffected {
                    rows_affected: None,
                    last_affected_id: None,
                });
                return;
            }
            if results.len() != prepared.columns.len() {
                Err(Error::msg(format!(
                    "Column/result mismatch: {} columns but {} redis results",
                    prepared.columns.len(),
                    results.len()
                )))
                .with_context(context)?;
                return;
            }
            if results.iter().all(|v| matches!(v, redis::Value::Nil)) {
                return;
            }
            let mut labels = Arc::new_zeroed_slice(prepared.columns.len());
            let mut values = Vec::with_capacity(prepared.columns.len());
            {
                let labels_mut = Arc::get_mut(&mut labels).unwrap();
                for (i, (col, mut redis_val)) in
                    prepared.columns.iter().zip(results.into_iter()).enumerate()
                {
                    labels_mut[i].write(col.name().into());
                    if let (tank_core::Value::Map(..), redis::Value::Array(array)) =
                        (&col.value, &mut redis_val)
                    {
                        redis_val = redis::Value::Map(
                            array
                                .chunks_mut(2)
                                .map(|v| (mem::take(&mut v[0]), mem::take(&mut v[1])))
                                .collect(),
                        )
                    }
                    let converted: ValueWrap = redis_val.try_into().with_context(context)?;
                    values.push(converted.0.into_owned());
                }
            }
            let row = QueryResult::Row(Row {
                labels: unsafe { labels.assume_init() },
                values: values.into(),
            });
            yield row;
        }
    }
}
