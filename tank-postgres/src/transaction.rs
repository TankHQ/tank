use crate::{PostgresConnection, PostgresDriver, PostgresPrepared, PostgresResults, ValueWrap};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Error, Executor, Query, RawQuery, Result, Transaction,
    future::TryFutureExt,
    stream::TryStreamExt,
};

/// Postgres transaction.
pub struct PostgresTransaction<'c>(pub(crate) tokio_postgres::Transaction<'c>);

impl<'c> PostgresTransaction<'c> {
    pub async fn new(client: &'c mut PostgresConnection) -> Result<Self> {
        Ok(Self(client.client.transaction().await.map_err(|e| {
            log::error!("{e:#}");
            e
        })?))
    }
}

impl<'c> Executor for PostgresTransaction<'c> {
    type Driver = PostgresDriver;

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<PostgresDriver>> {
        Ok(
            PostgresPrepared::new(self.0.prepare(&sql).await.map_err(|e| {
                let error = Error::new(e);
                log::error!("{error:#}");
                error
            })?)
            .into(),
        )
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl tank_core::Results>> + Send {
        async move {
            let mut query = query.as_query();
            let context = format!("While running the query:\n{}", query.as_mut());
            let result: Result<PostgresResults> = match query.as_mut() {
                Query::Raw(RawQuery { sql, .. }) => {
                    let rows = self
                        .0
                        .query_raw(sql.as_str(), Vec::<ValueWrap>::new())
                        .await
                        .map_err(|e| Error::new(e).context(context.clone()))?;
                    let mut rows = std::pin::pin!(rows);
                    let mut collected = Vec::new();
                    while let Some(row) = rows.try_next().await? {
                        collected.push(row);
                    }
                    let affected = rows.rows_affected();
                    PostgresResults::new(collected, affected)
                }
                Query::Prepared(prepared) => {
                    let mut params = prepared.take_params();
                    let types = prepared.statement.params();
                    for (i, param) in params.iter_mut().enumerate() {
                        *param = ValueWrap(Cow::Owned(
                            std::mem::take(param)
                                .0
                                .into_owned()
                                .try_as(&crate::util::postgres_type_to_value(&types[i]))?,
                        ));
                    }
                    let rows = self
                        .0
                        .query_raw(&prepared.statement, params)
                        .await
                        .map_err(|e| Error::new(e).context(context.clone()))?;
                    let mut rows = std::pin::pin!(rows);
                    let mut collected = Vec::new();
                    while let Some(row) = rows.try_next().await? {
                        collected.push(row);
                    }
                    let affected = rows.rows_affected();
                    PostgresResults::new(collected, affected)
                }
            };
            result.map_err(|e| {
                log::error!("{e:#}");
                e
            })
        }
    }
}

impl<'c> Transaction<'c> for PostgresTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        self.0.commit().map_err(|e| {
            let error = Error::new(e);
            log::error!("{error:#}");
            error
        })
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        self.0.rollback().map_err(|e| {
            let error = Error::new(e);
            log::error!("{error:#}");
            error
        })
    }
}
