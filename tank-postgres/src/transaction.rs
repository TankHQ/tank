use crate::{PostgresConnection, PostgresDriver, TankParam, rows::PostgresResults};
use std::future::Future;
use tank_core::{AsQuery, Error, Executor, Query, RawQuery, Result, Transaction};
use wtx::database::DbClient as _;

/// Postgres transaction.
///
/// `wtx` has no dedicated transaction object, so this holds a mutable borrow of
/// the connection and issues `BEGIN`/`COMMIT`/`ROLLBACK` explicitly.
pub struct PostgresTransaction<'c> {
    pub(crate) connection: &'c mut PostgresConnection,
}

impl<'c> PostgresTransaction<'c> {
    pub(crate) async fn new(connection: &'c mut PostgresConnection) -> Result<Self> {
        connection
            .client
            .execute_ignored("BEGIN")
            .await
            .map_err(Error::new)?;
        Ok(Self { connection })
    }
}

impl<'c> Executor for PostgresTransaction<'c> {
    type Driver = PostgresDriver;

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<PostgresDriver>> {
        let sql = sql.trim_end().trim_end_matches(';').to_string();
        Ok(crate::PostgresPrepared::new(sql).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl tank_core::Results>> + Send {
        async move {
            let mut query = query.as_query();
            let context = format!("While running the query:\n{}", query.as_mut());
            let owned = std::mem::take(query.as_mut());
            let result = match owned {
                Query::Raw(raw) => {
                    let r = run_on(self.connection, &raw.sql, &[]).await;
                    *query.as_mut() = Query::Raw(raw);
                    r
                }
                Query::Prepared(mut prepared) => {
                    let params = prepared.take_params()?;
                    let r = run_on(self.connection, &prepared.sql, &params).await;
                    *query.as_mut() = Query::Prepared(prepared);
                    r
                }
            };
            result.map_err(|e| {
                let error = e.context(context.clone());
                log::error!("{error:#}");
                error
            })
        }
    }
}

/// Runs one statement inside the current transaction.
async fn run_on<'s>(
    connection: &'s mut PostgresConnection,
    sql: &str,
    params: &[TankParam],
) -> Result<PostgresResults<'s>> {
    let stmt = crate::TankStmt::new(sql, params);
    let PostgresConnection { client, scratch } = connection;
    let records = client
        .execute_stmt_many(stmt, params, |_| Ok(()))
        .await
        .map_err(Error::new)?;
    scratch.prepare(&records)?;
    Ok(PostgresResults::new(
        &records,
        scratch.labels(),
        scratch.decoders(),
    ))
}

impl<'c> Transaction<'c> for PostgresTransaction<'c> {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        async move {
            self.connection
                .client
                .execute_ignored("COMMIT")
                .await
                .map_err(Error::new)
        }
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        async move {
            self.connection
                .client
                .execute_ignored("ROLLBACK")
                .await
                .map_err(Error::new)
        }
    }
}
