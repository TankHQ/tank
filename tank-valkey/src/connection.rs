use crate::{ValkeyDriver, ValkeyTransaction, ValueWrap};
use async_stream::try_stream;
use redis::{Client, aio::MultiplexedConnection};
use std::{borrow::Cow, future, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, Result, Row, RowLabeled,
    stream::Stream,
};

pub struct ValkeyConnection {
    pub(crate) connection: MultiplexedConnection,
}

impl Connection for ValkeyConnection {
    async fn connect(url: Cow<'static, str>) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!("While trying to connect to `{}`", url));
        let client = Client::open(&*url).map_err(|e| Error::msg(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::msg(e.to_string()))?;
        Ok(Self { connection })
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
            let raw_result = prepared
                .make_pipeline()
                .query_async::<redis::Value>(&mut self.connection)
                .await
                .map_err(Error::new)?;
            let results = match raw_result {
                redis::Value::Array(arr) => arr,
                redis::Value::Nil => vec![],
                redis::Value::ServerError(err) => {
                    Err(Error::msg(format!("Valkey/Redis server error: {err}")))?;
                    return;
                }
                other => {
                    Err(Error::msg(format!(
                        "Unexpected top-level pipeline response: {:?}",
                        other
                    )))?;
                    return;
                }
            };
            let is_single_hgetall = results.len() == 1 && prepared.columns.is_empty();
            if is_single_hgetall {
            } else {
                if results.len() != prepared.columns.len() {
                    Err(Error::msg(format!(
                        "Pipeline returned {} results but {} columns were selected",
                        results.len(),
                        prepared.columns.len()
                    )))?;
                    return;
                }
                let mut row_names = Arc::new_zeroed_slice(prepared.columns.len());
                let mut values: Row = Vec::with_capacity(prepared.columns.len()).into_boxed_slice();
                {
                    let row_names = Arc::get_mut(&mut row_names).unwrap();
                    for (i, (col_def, redis_val)) in
                        prepared.columns.iter().zip(results).enumerate()
                    {
                        row_names.get_mut(i).unwrap().write(col_def.name().into());
                        let converted: ValueWrap = redis_val.try_into()?;
                        *values.get_mut(i).unwrap() = converted.0.into_owned();
                    }
                }
                let row = QueryResult::Row(RowLabeled {
                    labels: unsafe { row_names.assume_init() },
                    values,
                });
                yield row;
            }
        }
    }
}
