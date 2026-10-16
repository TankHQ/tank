use crate::{MySQLDriver, MySQLPrepared, RowWrap};
use async_stream::try_stream;
use mysql_async::prelude::Queryable;
use std::future::Future;
use tank_core::{
    AsQuery, Error, ErrorContext, Executor, OwnedResults, Query, QueryResult, QueryResultOwned,
    RawQuery, Result, Results, RowsAffected,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};

pub(crate) struct MySQLQueryable<T: mysql_async::prelude::Queryable> {
    pub(crate) executor: T,
    pub(crate) driver: MySQLDriver,
}

impl<T: mysql_async::prelude::Queryable + Send> Executor for MySQLQueryable<T> {
    type Driver = MySQLDriver;

    fn driver(&self) -> MySQLDriver {
        self.driver
    }

    async fn do_prepare(&mut self, RawQuery { sql, .. }: RawQuery) -> Result<Query<MySQLDriver>> {
        let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
        let prepared = self
            .executor
            .prep(sql.as_str())
            .await
            .map_err(Error::new)
            .with_context(make_context)?;
        Ok(MySQLPrepared::new(prepared).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<MySQLDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl Results>> + Send {
        async move { OwnedResults::from_query_result_stream(self.do_run(query)).await }
    }
}

impl<T: Queryable + Send> MySQLQueryable<T> {
    fn do_run<'s, Q: AsQuery<MySQLDriver> + 's>(
        &'s mut self,
        query: Q,
    ) -> impl Stream<Item = Result<QueryResultOwned>> + Send + 's {
        let mut query = query.as_query();
        let context = format!("While running the query:\n{}", query.as_mut());
        try_stream! {
            match query.as_mut() {
                Query::Raw(RawQuery { sql, .. }) => {
                    let mut result = self.executor.query_iter(sql.as_str()).await?;
                    let mut rows = 0;
                    while let Some(mut stream) = result.stream::<RowWrap>().await? {
                        while let Some(row) = stream.next().await.transpose()? {
                            rows += 1;
                            yield QueryResult::Row(row.0)
                        }
                    }
                    let affected = result.affected_rows();
                    if rows == 0 && affected > 0 {
                        yield QueryResult::Affected(RowsAffected {
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
            log::error!("{error:#}");
            error
        })
    }
}
