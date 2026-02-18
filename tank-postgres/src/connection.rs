use crate::{
    PostgresDriver, PostgresPrepared, PostgresTransaction, ValueWrap,
    util::{
        postgres_type_to_value, stream_postgres_row_to_tank_row,
        stream_postgres_simple_query_message_to_tank_query_result, value_to_postgres_type,
    },
};
use async_stream::try_stream;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use postgres_types::ToSql;
use std::{
    borrow::Cow,
    env, mem,
    path::PathBuf,
    pin::{Pin, pin},
    str::FromStr,
};
use tank_core::{
    AsQuery, Connection, Driver, DynQuery, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, RawQuery, Result, RowsAffected, Transaction,
    future::Either,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};
use tokio::{spawn, task::JoinHandle};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};

/// PostgreSQL connection.
#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) client: tokio_postgres::Client,
    pub(crate) handle: JoinHandle<()>,
    pub(crate) _transaction: bool,
}

impl Executor for PostgresConnection {
    type Driver = PostgresDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<Self::Driver>> {
        let sql = sql.as_str().trim_end().trim_end_matches(';');
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
        let context = format!("While running the query:\n{}", query.as_mut());
        let mut owned = mem::take(query.as_mut());
        match owned {
            Query::Raw(raw) => Either::Left(try_stream! {
                let sql = &raw.0;
                {
                    let stream = stream_postgres_simple_query_message_to_tank_query_result(
                        async move || self.client.simple_query_raw(sql).await.map_err(Into::into),
                    );
                    let mut stream = pin!(stream);
                    while let Some(value) = stream.next().await.transpose()? {
                        yield value;
                    }
                }
                *query.as_mut() = Query::Raw(raw);
            }),
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
    ) -> impl Stream<Item = Result<tank_core::RowLabeled>> + Send {
        let mut query = query.as_query();
        let context = format!("While fetching the query:\n{}", query.as_mut());
        let owned = mem::take(query.as_mut());
        stream_postgres_row_to_tank_row(async move || {
            let row_stream = match owned {
                Query::Raw(RawQuery(sql)) => {
                    let stream = self
                        .client
                        .query_raw(&sql, Vec::<ValueWrap>::new())
                        .await
                        .map_err(|e| Error::new(e).context(context.clone()))?;
                    *query.as_mut() = Query::raw(sql);
                    stream
                }
                Query::Prepared(mut prepared) => {
                    let mut params = prepared.take_params();
                    let types = prepared.statement.params();

                    for (i, param) in params.iter_mut().enumerate() {
                        *param = ValueWrap(Cow::Owned(
                            mem::take(param)
                                .take_value()
                                .try_as(&postgres_type_to_value(&types[i]))?,
                        ));
                    }
                    let stream = self
                        .client
                        .query_raw(&prepared.statement, params)
                        .await
                        .map_err(|e| Error::new(e).context(context.clone()))?;
                    *query.as_mut() = Query::Prepared(prepared);
                    stream
                }
            };
            Ok(row_stream).map_err(|e| {
                log::error!("{:#}", e);
                e
            })
        })
    }

    async fn append<'a, E, It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        E: Entity + 'a,
        It: IntoIterator<Item = &'a E> + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        let context = || format!("While appending to the table `{}`", E::table().full_name());
        let mut result = RowsAffected {
            rows_affected: Some(0),
            last_affected_id: None,
        };
        let writer = self.driver().sql_writer();
        let mut query = DynQuery::default();
        writer.write_copy::<E>(&mut query);
        let sink = self
            .client
            .copy_in(&query.as_str() as &str)
            .await
            .with_context(context)?;
        let types: Vec<_> = E::columns()
            .into_iter()
            .map(|c| value_to_postgres_type(&c.value))
            .collect();
        let writer = BinaryCopyInWriter::new(sink, &types);
        let mut writer = pin!(writer);
        let columns_len = E::columns().len();
        let mut values = Vec::<ValueWrap>::with_capacity(columns_len);
        let mut refs = Vec::<&(dyn ToSql + Sync)>::with_capacity(columns_len);
        for entity in entities.into_iter() {
            values.extend(
                entity
                    .row_full()
                    .into_iter()
                    .map(|v| ValueWrap(Cow::Owned(v))),
            );
            refs.extend(
                values
                    .iter()
                    .map(|v| unsafe { &*(v as &(dyn ToSql + Sync) as *const _) }),
            );
            Pin::as_mut(&mut writer)
                .write(&refs)
                .await
                .with_context(context)?;
            refs.clear();
            values.clear();
            *result.rows_affected.as_mut().unwrap() += 1;
        }
        writer.finish().await.with_context(context)?;
        Ok(result)
    }
}

impl Connection for PostgresConnection {
    async fn connect(url: Cow<'static, str>) -> Result<PostgresConnection> {
        let context = format!("While trying to connect to `{}`", truncate_long!(url));
        let mut url = Self::sanitize_url(url)?;
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
            .with_context(|| context.clone())?;
            if path.exists() {
                builder.set_ca_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param("sslcert", "PGSSLCERT", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.crt"),
            )
            .with_context(|| context.clone())?;
            if path.exists() {
                builder.set_certificate_chain_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param("sslkey", "PGSSLKEY", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.key"),
            )
            .with_context(|| context.clone())?;
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

    fn begin(&mut self) -> impl Future<Output = Result<PostgresTransaction<'_>>> {
        PostgresTransaction::new(self)
    }

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
