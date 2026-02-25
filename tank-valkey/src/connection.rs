use crate::{ValkeyDriver, value_wrap::ValueWrap};
use async_stream::try_stream;
use redis::{Client, aio::MultiplexedConnection};
use std::{borrow::Cow, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, Result, RowLabeled,
    Value,
    stream::{Stream, TryStreamExt},
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
        let client = Client::open(&*url).with_context(|| context.clone())?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| context.clone())?;
        Ok(Self { connection })
    }
    fn begin(
        &mut self,
    ) -> impl Future<Output = tank_core::Result<<Self::Driver as tank_core::Driver>::Transaction<'_>>>
    {
        todo!()
    }
}

impl Executor for ValkeyConnection {
    type Driver = ValkeyDriver;

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
                Err(Error::msg(
                    "Query is not the expected tank::Query::Prepared variant (Valkey driver uses prepared)",
                ))?;
                return;
            };
            let command = prepared.get_command();
            let (labels, values): (Vec<String>, Vec<Value>) = command
                .query_async::<Vec<(String, ValueWrap)>>(&mut self.connection)
                .await
                .map_err(|e| Error::msg(e.to_string()))?
                .into_iter().map(|(k,v)| (k, v.0)).unzip();
            yield QueryResult::Row(RowLabeled { labels: labels.into(), values: values.into() });
        }
        .map_err(move |e: Error| {
            log::error!("{e:#}");
            e
        })
    }
}
