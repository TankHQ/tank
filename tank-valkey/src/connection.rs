use crate::{ValkeyDriver, ValkeyPrepared, ValkeyTransaction, ValueWrap};
use anyhow::anyhow;
use async_stream::try_stream;
use redis::{Client, aio::MultiplexedConnection};
use std::{borrow::Cow, future, mem, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, QueryResultOwned,
    Result, Row, RowOwned, RowsAffected, describe_url, stream::Stream, truncate_long,
};
#[derive(Debug)]
pub struct ValkeyConnection {
    driver: ValkeyDriver,
    pub(crate) connection: MultiplexedConnection,
}

pub type RedisConnection = ValkeyConnection;

impl Connection for ValkeyConnection {
    async fn connect(driver: &ValkeyDriver, url: Cow<'static, str>) -> Result<Self>
    where
        Self: Sized,
    {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to Valkey {}",
                describe_url::<ValkeyDriver>(&url)
            )
        };
        let client = Client::open(url.as_str()).map_err(Error::new)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(Error::new)
            .with_context(make_context)?;
        Ok(Self {
            driver: *driver,
            connection,
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ValkeyTransaction<'_>>> + Send {
        future::ready(Ok(ValkeyTransaction {
            connection: self,
            commands: Default::default(),
        }))
    }
}

impl Executor for ValkeyConnection {
    type Driver = ValkeyDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    fn driver(&self) -> Self::Driver
    where
        Self: Sized,
    {
        self.driver
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ValkeyDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        let mut query = query.as_query();
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
                Err(anyhow!(
                    "Query is not the expected tank::Query::Prepared variant (Valkey/Redis driver uses prepared)",
                ))?;
                return;
            };
            if prepared.is_empty() {
                if let Some(result) = decode_result(prepared, redis::Value::Nil)? {
                    yield result;
                }
                return;
            }
            let pipeline = prepared.make_pipeline();
            let make_context = || {
                format!(
                    "While executing the query: {}",
                    truncate_long!(format!("{pipeline:?}"))
                )
            };
            let raw_result = pipeline
                .query_async::<redis::Value>(&mut self.connection)
                .await
                .map_err(Error::new)
                .with_context(make_context)?;
            if let Some(result) = decode_result(prepared, raw_result).with_context(make_context)? {
                yield result;
            }
        }
    }
}

// A pipeline contains all native commands for one logical query, not one result per command.
fn decode_result(
    prepared: &ValkeyPrepared,
    raw_result: redis::Value,
) -> Result<Option<QueryResultOwned>> {
    let results = match raw_result {
        redis::Value::Array(arr) => arr,
        redis::Value::Nil => vec![],
        redis::Value::ServerError(err) => {
            return Err(anyhow!("Valkey/Redis server error: {err}"));
        }
        other => {
            return Err(anyhow!(
                "Unexpected top-level pipeline response: {:?}",
                other
            ));
        }
    };
    if !prepared.returns_rows {
        return Ok(Some(QueryResult::Affected(RowsAffected {
            rows_affected: None,
            last_affected_id: None,
        })));
    }
    if results.is_empty() || results.iter().all(|v| matches!(v, redis::Value::Nil)) {
        return Ok(None);
    }
    if prepared.columns.is_empty() {
        let mut results = results.into_iter();
        let fields = match (results.next(), results.next()) {
            (Some(redis::Value::Map(fields)), None) => fields,
            (Some(redis::Value::Array(fields)), None) if fields.len() % 2 == 0 => {
                let mut fields = fields.into_iter();
                std::iter::from_fn(|| Some((fields.next()?, fields.next()?))).collect()
            }
            result => return Err(anyhow!("Unexpected HGETALL response: {result:?}")),
        };
        if fields.is_empty() {
            return Ok(None);
        }
        let mut labels = Vec::with_capacity(fields.len());
        let mut values = Vec::with_capacity(fields.len());
        for (label, value) in fields {
            let label: ValueWrap = label.try_into()?;
            let tank_core::Value::Varchar(Some(label)) = label.0.into_owned() else {
                return Err(anyhow!("Expected a string HGETALL field name"));
            };
            labels.push(label.into_owned());
            values.push(ValueWrap::try_from(value)?.0.into_owned().into_owned());
        }
        return Ok(Some(QueryResult::Row(RowOwned::new(labels.into(), values))));
    }
    if results.len() != prepared.columns.len() {
        return Err(anyhow!(
            "Column/result mismatch: {} columns but {} redis results",
            prepared.columns.len(),
            results.len()
        ));
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
            let converted: ValueWrap = redis_val.try_into()?;
            values.push(converted.0.into_owned());
        }
    }
    let row = RowOwned {
        names: unsafe { labels.assume_init() },
        values,
    };
    Ok(Some(QueryResult::Row(row)))
}
