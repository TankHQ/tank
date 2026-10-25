use crate::{PostgresConnection, PostgresDriver};
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

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<PostgresDriver>> {
        let sql = sql.trim_end().trim_end_matches(';').to_string();
        Ok(crate::PostgresPrepared::new(sql).into())
    }

    fn run<'s, Q>(
        &'s mut self,
        query: Q,
    ) -> impl Future<
        Output = Result<<Self::Driver as tank_core::Driver>::Results<'s, Q::Output>>,
    > + Send
    where
        Q: AsQuery<PostgresDriver> + Send + 's,
    {
        async move {
            let mut query = query.as_query();
            let result = match query.as_mut() {
                Query::Raw(raw) => self.connection.run_prepared(&raw.sql, &[]).await,
                Query::Prepared(prepared) => {
                    prepared.encode_params()?;
                    self.connection
                        .run_prepared(&prepared.sql, &prepared.encoded_params)
                        .await
                }
            };
            result.map_err(|e| {
                let error = e.context(format!("While running the query:\n{}", query.as_mut()));
                log::error!("{error:#}");
                error
            })
        }
    }
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
