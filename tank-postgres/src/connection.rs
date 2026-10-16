use crate::{
    PostgresDriver, PostgresPrepared, PostgresResults, PostgresTransaction, ValueWrap,
    util::{postgres_type_to_value, value_to_postgres_type},
};
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
    AsEntity, AsQuery, Connection, Driver, DynQuery, Entity, Error, ErrorContext, Executor, Query,
    RawQuery, Result, RowsAffected, SqlCoreWriter, describe_url,
    stream::TryStreamExt,
    truncate_long,
};
use tokio::{spawn, task::JoinHandle};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};
use url::Url;

/// PostgreSQL connection.
#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) client: tokio_postgres::Client,
    pub(crate) handle: JoinHandle<()>,
}

impl Connection for PostgresConnection {    async fn connect(driver: &PostgresDriver, url: Cow<'static, str>) -> Result<Self> {
        let mut url = Self::sanitize_url(driver, url)?;
        let make_context = |url: &Url| {
            format!(
                "While trying to connect to Postgres {}",
                describe_url::<PostgresDriver>(url)
            )
        };
        let take_url_param = |url: &mut Url, key: &str, env_var: &str, remove: bool| {
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
                *url = result;
            };
            value.or_else(|| env::var(env_var).ok().map(Into::into))
        };
        let sslmode =
            take_url_param(&mut url, "sslmode", "PGSSLMODE", false).unwrap_or("disable".into());
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
                take_url_param(&mut url, "sslrootcert", "PGSSLROOTCERT", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/root.crt"),
            )
            .with_context(|| make_context(&url))?;
            if path.exists() {
                builder.set_ca_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param(&mut url, "sslcert", "PGSSLCERT", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.crt"),
            )
            .with_context(|| make_context(&url))?;
            if path.exists() {
                builder.set_certificate_chain_file(path)?;
            }
            let path = PathBuf::from_str(
                take_url_param(&mut url, "sslkey", "PGSSLKEY", true)
                    .as_deref()
                    .unwrap_or("~/.postgresql/postgresql.key"),
            )
            .with_context(|| make_context(&url))?;
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
        Ok(Self { client, handle })
    }

    fn begin(&mut self) -> impl Future<Output = Result<PostgresTransaction<'_>>> + Send {
        PostgresTransaction::new(self)
    }

    async fn disconnect(self) -> Result<()> {
        drop(self.client);
        if let Err(e) = self.handle.await {
            let error = Error::new(e).context("While disconnecting from Postgres");
            log::error!("{error:#}");
            return Err(error);
        }
        Ok(())
    }
}

impl PostgresConnection {
    /// Runs a single-statement raw query through the typed protocol, so column
    /// values (arrays in particular) are decoded with full type information.
    async fn run_typed(
        &self,
        sql: &str,
        context: &str,
    ) -> Result<PostgresResults> {
        let rows = self
            .client
            .query_raw(sql, Vec::<ValueWrap>::new())
            .await
            .map_err(|e| Error::new(e).context(context.to_string()))?;
        let mut rows = pin!(rows);
        let mut collected = Vec::new();
        while let Some(row) = rows.try_next().await? {
            collected.push(row);
        }
        let affected = rows.rows_affected();
        PostgresResults::new(collected, affected)
    }

    /// Runs raw SQL through the simple protocol, which (unlike the extended
    /// protocol) accepts several semicolon-separated statements. Values are
    /// decoded as text/unknown because the simple protocol carries no type
    /// information.
    async fn run_simple(&mut self, sql: &str, context: &str) -> Result<PostgresResults> {
        let messages = self
            .client
            .simple_query_raw(sql)
            .await
            .map_err(|e| Error::new(e).context(context.to_string()))?;
        let mut messages = pin!(messages);
        let mut labels: Option<tank_core::RowLabels> = None;
        let mut arena: Vec<tank_core::ValueOwned> = Vec::new();
        let mut spans: Vec<std::ops::Range<usize>> = Vec::new();
        let mut row_labels: Vec<tank_core::RowLabels> = Vec::new();
        let mut items: Vec<crate::rows::Item> = Vec::new();
        while let Some(message) = messages.try_next().await? {
            match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    let labels = labels.get_or_insert_with(|| {
                        row.columns()
                            .iter()
                            .map(|c| c.name().to_string())
                            .collect()
                    });
                    let start = arena.len();
                    let decoded = crate::util::simple_query_row_to_tank_row_owned(labels, row)?;
                    arena.extend(decoded.values.into_owned());
                    spans.push(start..arena.len());
                    row_labels.push(labels.clone());
                    items.push(crate::rows::Item::Row(spans.len() - 1));
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(rows_affected) => {
                    items.push(crate::rows::Item::Affected(tank_core::RowsAffected {
                        rows_affected: Some(rows_affected),
                        last_affected_id: None,
                    }));
                }
                _ => {}
            }
        }
        Ok(PostgresResults::from_parts(arena, spans, items, row_labels))
    }
}

impl Executor for PostgresConnection {
    type Driver = PostgresDriver;

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<PostgresDriver>> {
        let sql = sql.as_str().trim_end().trim_end_matches(';');
        let make_context = || format!("While preparing the query:\n{}", truncate_long!(sql));
        let prepared = self
            .client
            .prepare(sql)
            .await
            .map_err(Error::new)
            .with_context(make_context)?;
        Ok(PostgresPrepared::new(prepared).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl tank_core::Results>> + Send {
        async move {
            let mut query = query.as_query();
            let context = format!("While running the query:\n{}", query.as_mut());
            let owned = mem::take(query.as_mut());
            let result: Result<PostgresResults> = match owned {
                Query::Raw(raw) => {
                    let result = if crate::util::has_multiple_statements(&raw.sql) {
                        // The extended protocol cannot run several commands in
                        // one prepared statement, so multi-statement raw SQL
                        // goes through the simple protocol.
                        self.run_simple(&raw.sql, &context).await
                    } else {
                        self.run_typed(&raw.sql, &context).await
                    };
                    *query.as_mut() = Query::Raw(raw);
                    result
                }
                Query::Prepared(mut prepared) => {
                    let mut params = prepared.take_params();
                    let types = prepared.statement.params();
                    for (i, param) in params.iter_mut().enumerate() {
                        *param = ValueWrap(Cow::Owned(
                            mem::take(param)
                                .0
                                .into_owned()
                                .try_as(&postgres_type_to_value(&types[i]))?,
                        ));
                    }
                    let rows = self
                        .client
                        .query_raw(&prepared.statement, params)
                        .await
                        .map_err(|e| Error::new(e).context(context.clone()))?;
                    let mut rows = pin!(rows);
                    let mut collected = Vec::new();
                    while let Some(row) = rows.try_next().await? {
                        collected.push(row);
                    }
                    let affected = rows.rows_affected();
                    *query.as_mut() = Query::Prepared(prepared);
                    PostgresResults::new(collected, affected)
                }
            };
            result.map_err(|e| {
                log::error!("{e:#}");
                e
            })
        }
    }

    async fn append<It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let mut iter = entities.into_iter();
        let writer = self.driver().sql_writer();
        let make_context = || {
            format!(
                "While appending to the table `{}`",
                E::<It>::table().full_name(writer.separator())
            )
        };
        let mut query = DynQuery::default();
        writer.write_copy::<E<It>>(&mut query);
        let sink = match self
            .client
            .copy_in(&query.as_str() as &str)
            .await
            .with_context(make_context)
        {
            Ok(v) => v,
            Err(e) => {
                log::error!("{e:#}");
                return Err(e);
            }
        };
        let types: Vec<_> = E::<It>::columns()
            .into_iter()
            .map(|c| value_to_postgres_type(&c.value))
            .collect();
        let writer = BinaryCopyInWriter::new(sink, &types);
        let mut writer = pin!(writer);
        let columns_len = E::<It>::columns().len();
        let mut values = Vec::<ValueWrap>::with_capacity(columns_len);
        let mut refs = Vec::<&(dyn ToSql + Sync)>::with_capacity(columns_len);
        loop {
            match iter.next() {
                None => break,
                Some(entity) => {
                    values.extend(
                        entity
                            .as_entity()
                            .row_values()
                            .into_owned()
                            .into_iter()
                            .map(|v| ValueWrap(Cow::Owned(v))),
                    );
                }
            }
            refs.extend(
                values
                    .iter()
                    .map(|v| unsafe { &*(v as &(dyn ToSql + Sync) as *const _) }),
            );
            match Pin::as_mut(&mut writer)
                .write(&refs)
                .await
                .with_context(make_context)
            {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{e:#}");
                    return Err(e);
                }
            };
            refs.clear();
            values.clear();
        }
        match writer.finish().await.with_context(make_context) {
            Ok(v) => Ok(RowsAffected {
                rows_affected: Some(v),
                last_affected_id: None,
            }),
            Err(e) => {
                log::error!("{e:#}");
                return Err(e);
            }
        }
    }
}
