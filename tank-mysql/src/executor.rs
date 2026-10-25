use crate::{MySQLDriver, MySQLPrepared, RowWrap};
use async_stream::try_stream;
use mysql_async::prelude::Queryable;
use std::future::Future;
use tank_core::{
    AsQuery, Error, ErrorContext, Executor, OwnedResults, Query, QueryResult, QueryResultOwned,
    RawQuery, Result, RowsAffected,
    stream::{Stream, TryStreamExt},
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

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as tank_core::Driver>::Results<'s, Q::Output>>>
    + Send
    where
        Q: AsQuery<MySQLDriver> + Send + 's,
    {
        async move {
            let results = self.do_run(query).try_collect::<Vec<_>>().await?;
            Ok(OwnedResults::new(results.into_iter().map(Ok).collect()))
        }
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
            // Stored routines can emit several result sets; preserve all server outputs.
            match query.as_mut() {
                Query::Raw(RawQuery { sql, .. }) => {
                    let mut result = self.executor.query_iter(sql.as_str()).await?;
                    while let Some(stream) = result.stream::<RowWrap>().await? {
                        // Draining advances the parent result, so capture this set's metadata first.
                        let has_columns = !stream.columns_ref().is_empty();
                        let affected = RowsAffected {
                            rows_affected: Some(stream.affected_rows()),
                            last_affected_id: stream.last_insert_id().map(|v| v as _),
                        };
                        let rows = stream.map_ok(|row| row.0).try_collect::<Vec<_>>().await?;
                        yield if has_columns {
                            QueryResult::Rows(rows.into_iter())
                        } else {
                            QueryResult::Affected(affected)
                        };
                    }
                }
                Query::Prepared(prepared) => {
                    let params = prepared.take_params()?;
                    let mut result = self
                        .executor
                        .exec_iter(&prepared.statement, params)
                        .await?;
                    while let Some(stream) = result.stream::<RowWrap>().await? {
                        let has_columns = !stream.columns_ref().is_empty();
                        let affected = RowsAffected {
                            rows_affected: Some(stream.affected_rows()),
                            last_affected_id: stream.last_insert_id().map(|v| v as _),
                        };
                        let rows = stream.map_ok(|row| row.0).try_collect::<Vec<_>>().await?;
                        yield if has_columns {
                            QueryResult::Rows(rows.into_iter())
                        } else {
                            QueryResult::Affected(affected)
                        };
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
