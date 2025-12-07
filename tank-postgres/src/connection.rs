use crate::{
    PostgresDriver, PostgresPrepared, PostgresTransaction, ValueWrap, postgres_type_to_value,
    util::{row_to_tank_row, stream_postgres_simple_query_message_to_tank_query_result},
};
use async_stream::try_stream;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{borrow::Cow, env, mem, path::PathBuf, pin::pin, str::FromStr, sync::Arc};
use tank_core::{
    AsQuery, Connection, Driver, Error, ErrorContext, Executor, Query, QueryResult, Result,
    Transaction,
    future::Either,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};
use tokio::{spawn, task::JoinHandle};
use tokio_postgres::NoTls;
use url::Url;
use urlencoding::decode;

#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) client: tokio_postgres::Client,
    pub(crate) handle: JoinHandle<()>,
    pub(crate) _transaction: bool,
}

impl Executor for PostgresConnection {
    type Driver = PostgresDriver;

    fn driver(&self) -> &Self::Driver {
        &PostgresDriver {}
    }

    async fn prepare(&mut self, sql: String) -> Result<Query<Self::Driver>> {
        let sql = sql.trim_end().trim_end_matches(';');
        Ok(
            PostgresPrepared::new(self.client.prepare(&sql).await.map_err(|e| {
                let error = Error::new(e).context(format!(
                    "While preparing the query:\n{}",
                    truncate_long!(sql)
                ));
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
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        let mut owned = mem::take(query.as_mut());
        match owned {
            Query::Raw(sql) => {
                Either::Left(stream_postgres_simple_query_message_to_tank_query_result(
                    async move || self.client.simple_query_raw(&sql).await.map_err(Into::into),
                ))
            }
            Query::Prepared(..) => Either::Right(try_stream! {
                let mut transaction = self.begin().await?;
                {
                    let mut stream = pin!(transaction.run(&mut owned));
                    while let Some(value) = stream.next().await.transpose()? {
                        yield value;
                    }
                }
                transaction.commit().await?;
                *query.as_mut() = mem::take(&mut owned);
            }),
        }
        .map_err(move |e: Error| {
            let error = e.context(context.clone());
            log::error!("{:#}", error);
            error
        })
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<tank_core::RowLabeled>> + Send + 's {
        let mut query = query.as_query();
        let context = Arc::new(format!("While fetching the query:\n{}", query.as_mut()));
        let owned = mem::take(query.as_mut());
        match owned {
            Query::Raw(mut sql) => {
                let stream = try_stream! {
                    let stream = self
                        .client
                        .query_raw(&sql, Vec::<ValueWrap>::new())
                        .await
                        .map_err(|e| {
                            let error = Error::new(e).context(context.clone());
                            log::error!("{:#}", error);
                            error
                        })?;
                    let mut stream = pin!(stream);
                    let mut labels: Option<tank_core::RowNames> = None;
                    while let Some(row) = stream.next().await.transpose()? {
                        let labels = labels.get_or_insert_with(|| {
                            row.columns().iter().map(|c| c.name().to_string()).collect()
                        });
                        yield tank_core::RowLabeled {
                            labels: labels.clone(),
                            values: row_to_tank_row(row)?,
                        };
                    }
                    *query.as_mut() = Query::Raw(mem::take(&mut sql))
                };
                Either::Left(stream)
            }
            Query::Prepared(mut prepared) => {
                let mut params = mem::take(&mut prepared.params);
                let stream = try_stream! {
                    let types = prepared.statement.params();
                    for (i, param) in params.iter_mut().enumerate() {
                        *param = ValueWrap(mem::take(param).0.try_as(&postgres_type_to_value(&types[i]))?);
                    }
                    let stream: tokio_postgres::RowStream = self
                        .client
                        .query_raw(&prepared.statement, params)
                        .await
                        .map_err(|e| {
                            let error = Error::new(e).context(context.clone());
                            log::error!("{:#}", error);
                            error
                        })?;
                    let mut stream = pin!(stream);
                    let mut labels: Option<tank_core::RowNames> = None;
                    while let Some(row) = stream.next().await.transpose()? {
                        let labels = labels.get_or_insert_with(|| {
                            row.columns().iter().map(|c| c.name().to_string()).collect()
                        });
                        yield tank_core::RowLabeled {
                            labels: labels.clone(),
                            values: row_to_tank_row(row)?,
                        };
                    }
                    *query.as_mut() = Query::Prepared(prepared)
                };
                Either::Right(stream)
            }
        }
    }
}

impl Connection for PostgresConnection {
    #[allow(refining_impl_trait)]
    async fn connect(url: Cow<'static, str>) -> Result<PostgresConnection> {
        let context = || format!("While trying to connect to `{}`", truncate_long!(url));
        let url = decode(&url).with_context(context)?;
        let prefix = format!("{}://", <Self::Driver as Driver>::NAME);
        if !url.starts_with(&prefix) {
            let error = Error::msg(format!(
                "Postgres connection url must start with `{}`",
                &prefix
            ))
            .context(context());
            log::error!("{:#}", error);
            return Err(error);
        }
        let mut url = Url::parse(&url).with_context(context)?;
        let mut take_url_param = |key: &str, env_var: &str, remove: bool| {
            let value = url
                .query_pairs()
                .find_map(|(k, v)| if k == key { Some(v) } else { None })
                .map(|v| v.to_string());
            if remove && let Some(..) = value {
                let mut result = url.clone();
                result.set_query(None);
                result
                    .query_pairs_mut()
                    .extend_pairs(url.query_pairs().filter(|(k, _)| k != key));
                url = result;
            };
            value.or_else(|| env::var(env_var).ok().map(Into::into))
        };
        let sslmode = take_url_param("sslmode", "PGSSLMODE", false).unwrap_or("disable".into());
        let (client, handle) = if sslmode == "disable" {
            let (client, connection) = tokio_postgres::connect(url.as_str(), NoTls).await?;
            let handle = spawn(async move {
                if let Err(error) = connection.await
                    && !error.is_closed()
                {
                    log::error!("Postgres connection error: {:#?}", error);
                }
            });
            (client, handle)
        } else {
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            let path = PathBuf::from_str(
                take_url_param("sslrootcert", "PGSSLROOTCERT", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/root.crt"),
            )
            .context(context())?;
            if path.exists() {
                builder.set_ca_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param("sslcert", "PGSSLCERT", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.crt"),
            )
            .context(context())?;
            if path.exists() {
                builder.set_certificate_chain_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param("sslkey", "PGSSLKEY", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.key"),
            )
            .context(context())?;
            if path.exists() {
                builder.set_private_key_file(path, SslFiletype::PEM)?;
            }
            builder.set_verify(SslVerifyMode::PEER);
            let connector = MakeTlsConnector::new(builder.build());
            let (client, connection) = tokio_postgres::connect(url.as_str(), connector).await?;
            let handle = spawn(async move {
                if let Err(error) = connection.await
                    && !error.is_closed()
                {
                    log::error!("Postgres connection error: {:#?}", error);
                }
            });
            (client, handle)
        };
        Ok(Self {
            client,
            handle,
            _transaction: false,
        })
    }

    #[allow(refining_impl_trait)]
    fn begin(&mut self) -> impl Future<Output = Result<PostgresTransaction<'_>>> {
        PostgresTransaction::new(self)
    }

    #[allow(refining_impl_trait)]
    async fn disconnect(self) -> Result<()> {
        drop(self.client);
        if let Err(e) = self.handle.await {
            let error = Error::new(e).context("While disconnecting from Postgres");
            log::error!("{:#}", error);
            return Err(error);
        }
        Ok(())
    }
}
