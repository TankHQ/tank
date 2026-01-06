use crate::{MySQLDriver, MySQLPrepared, RowWrap};
use async_stream::try_stream;
use std::sync::Arc;
use tank_core::{
    AsQuery, Error, Executor, Query, Result,
    stream::{Stream, StreamExt, TryStreamExt},
};

pub(crate) struct MySQLQueryable<T: mysql_async::prelude::Queryable> {
    pub(crate) executor: T,
}

impl<T: mysql_async::prelude::Queryable> Executor for MySQLQueryable<T> {
    type Driver = MySQLDriver;

    async fn prepare(&mut self, query: String) -> Result<Query<Self::Driver>> {
        Ok(MySQLPrepared::new(self.executor.prep(query).await?).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<tank_core::QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        try_stream! {
            match query.as_mut() {
                Query::Raw(sql) => {
                    let sql = sql.as_str();
                    let mut result = self.executor.query_iter(sql).await?;
                    let mut rows = 0;
                    while let Some(mut stream) = result.stream::<RowWrap>().await? {
                        while let Some(row) = stream.next().await.transpose()? {
                            rows += 1;
                            yield tank_core::QueryResult::Row(row.0)
                        }
                    }
                    let affected = result.affected_rows();
                    if rows == 0 && affected > 0 {
                        yield tank_core::QueryResult::Affected(tank_core::RowsAffected {
                            rows_affected: Some(affected),
                            last_affected_id: result.last_insert_id().map(|v| v as _),
                        });
                    }
                }
                Query::Prepared(prepared) => {
                    let params = prepared.take_params()?;
                    let mut stream = self
                        .executor
                        .exec_stream::<RowWrap, _, _>(&prepared.statement, params)
                        .await?;
                    while let Some(row) = stream.next().await.transpose()? {
                        yield row.0.into()
                    }
                }
            }
        }
        .map_err(move |e: Error| {
            let error = e.context(context.clone());
            log::error!("{:#}", error);
            error
        })
    }
}
