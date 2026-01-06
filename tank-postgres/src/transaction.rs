use crate::{
    PostgresConnection, PostgresDriver, PostgresPrepared, ValueWrap,
    util::stream_postgres_row_to_tank_row,
};
use std::mem;
use tank_core::{
    AsQuery, Error, Executor, Query, QueryResult, Result, Transaction,
    future::{Either, TryFutureExt},
    stream::{Stream, TryStreamExt},
};

pub struct PostgresTransaction<'c>(pub(crate) tokio_postgres::Transaction<'c>);

impl<'c> PostgresTransaction<'c> {
    pub async fn new(client: &'c mut PostgresConnection) -> Result<Self> {
        Ok(Self(client.client.transaction().await.map_err(|e| {
            log::error!("{:#}", e);
            e
        })?))
    }
}

impl<'c> Executor for PostgresTransaction<'c> {
    type Driver = PostgresDriver;

    async fn prepare(&mut self, query: String) -> Result<Query<Self::Driver>> {
        Ok(
            PostgresPrepared::new(self.0.prepare(&query).await.map_err(|e| {
                let error = Error::new(e);
                log::error!("{:#}", error);
                error
            })?)
            .into(),
        )
    }
    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let mut owned = mem::take(query.as_mut());
        stream_postgres_row_to_tank_row(async move || match &mut owned {
            Query::Raw(sql) => {
                let stream = self.0.query_raw(sql, Vec::<ValueWrap>::new()).await?;
                Ok(Either::Left(stream))
            }
            Query::Prepared(prepared) => {
                let params = prepared.take_params();
                let portal = self
                    .0
                    .bind_raw(&prepared.statement, params.into_iter())
                    .await?;
                Ok(Either::Right(self.0.query_portal_raw(&portal, 0).await?))
            }
        })
        .map_err(|e| {
            log::error!("{:#}", e);
            e
        })
    }
}

impl<'c> Transaction<'c> for PostgresTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> {
        self.0.commit().map_err(|e| {
            let e = Error::new(e);
            log::error!("{:#}", e);
            e
        })
    }

    fn rollback(self) -> impl Future<Output = Result<()>> {
        self.0.rollback().map_err(|e| {
            let e = Error::new(e);
            log::error!("{:#}", e);
            e
        })
    }
}
