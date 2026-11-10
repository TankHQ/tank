use crate::{
    PostgresDriver, PostgresPrepared, PostgresRows, PostgresStream, PostgresTransaction, ValueWrap,
    util::{stream_postgres_simple_query_message_to_tank_query_result, value_to_postgres_type},
};
use async_stream::try_stream;
use postgres_types::ToSql;
use rustls::{
    ClientConfig, RootCertStore,
    crypto::{CryptoProvider, aws_lc_rs::default_provider},
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{
    borrow::Cow,
    env, fs,
    iter::zip,
    mem,
    path::{Path, PathBuf},
    pin::{Pin, pin},
    sync::Arc,
};
use tank_core::{
    AsEntity, AsQuery, Connection, Driver, DynQuery, Entity, Error, ErrorContext, Executor, Query,
    QueryResult, RawQuery, Result, Row, RowLabels, RowOwned, RowsAffected, SqlWriter, Transaction,
    anyhow,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};
use tokio::{spawn, task::JoinHandle};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter};
use tokio_postgres_rustls::MakeRustlsConnect;

/// PostgreSQL connection.
#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) client: tokio_postgres::Client,
    pub(crate) handle: JoinHandle<()>,
    pub(crate) labels: RowLabels,
}

impl PostgresConnection {
    fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let pem = fs::read(path)
            .with_context(|| format!("Could not read certificate file {}", path.display()))?;
        let certificates = CertificateDer::pem_slice_iter(&pem)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("Invalid PEM certificate file {}", path.display()))?;
        if certificates.is_empty() {
            return Err(anyhow!("No certificates in {}", path.display()));
        }
        Ok(certificates)
    }

    fn tls_config(
        root: Option<String>,
        cert: Option<String>,
        key: Option<String>,
    ) -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
        let root_path = PathBuf::from(root.as_deref().unwrap_or("~/.postgresql/root.crt"));
        if root.is_some() || root_path.exists() {
            for certificate in Self::read_certificates(&root_path)? {
                roots.add(certificate)?;
            }
        }
        let provider = CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(default_provider()));
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots);
        let cert_path = PathBuf::from(cert.as_deref().unwrap_or("~/.postgresql/postgresql.crt"));
        let key_path = PathBuf::from(key.as_deref().unwrap_or("~/.postgresql/postgresql.key"));
        match (
            cert.is_some() || cert_path.exists(),
            key.is_some() || key_path.exists(),
        ) {
            (false, false) => Ok(builder.with_no_client_auth()),
            (true, true) => {
                let certificates = Self::read_certificates(&cert_path)?;
                let key = PrivateKeyDer::from_pem_file(&key_path)
                    .with_context(|| format!("Invalid private key file {}", key_path.display()))?;
                Ok(builder.with_client_auth_cert(certificates, key)?)
            }
            _ => Err(anyhow!(
                "Both sslcert and sslkey are required for client authentication"
            )),
        }
    }

    #[cold]
    #[inline(never)]
    fn query_error(e: impl Into<Error>, query: &Query<PostgresDriver>) -> Error {
        let error = e
            .into()
            .context(format!("While running the query:\n{query}"));
        log::error!("{error:#}");
        error
    }

    #[cold]
    #[inline(never)]
    fn rebuild_labels(labels: &mut RowLabels, columns: &[tokio_postgres::Column]) {
        *labels = columns.iter().map(|c| c.name().to_string()).collect();
    }

    #[inline]
    fn prepare_labels(labels: &mut RowLabels, columns: &[tokio_postgres::Column]) {
        if columns.len() != labels.len()
            || !zip(columns.iter(), labels.iter()).all(|(col, label)| col.name() == label)
        {
            Self::rebuild_labels(labels, columns);
        }
    }

    #[inline]
    async fn fetch_inner<'s>(
        &'s mut self,
        query: &mut Query<PostgresDriver>,
    ) -> Result<PostgresRows<'s>> {
        match query {
            Query::Raw(RawQuery(sql)) => {
                let mut stream = self
                    .client
                    .query_raw(sql.as_str(), Vec::<ValueWrap>::new())
                    .await
                    .map_err(|e| Self::query_error(e, query))?;
                let first = std::future::poll_fn(|cx| {
                    unsafe { Pin::new_unchecked(&mut stream) }.poll_next(cx)
                })
                .await
                .transpose()
                .map_err(|e| Self::query_error(e, query))?;
                if let Some(ref row) = first {
                    Self::prepare_labels(&mut self.labels, row.columns());
                    Ok(PostgresRows::new(first, stream, &self.labels))
                } else {
                    // The pre-poll above drained a zero-row result: polling the
                    // exhausted `RowStream` would yield `Error::closed()`.
                    Ok(PostgresRows::new_drained(stream, &self.labels))
                }
            }
            Query::Prepared(prepared) => {
                let params = match prepared.take_params() {
                    Ok(params) => params,
                    Err(e) => return Err(Self::query_error(e, query)),
                };
                if !Arc::ptr_eq(&self.labels, &prepared.labels) {
                    self.labels = prepared.labels.clone();
                }
                let stream = self
                    .client
                    .query_raw(&prepared.statement, params)
                    .await
                    .map_err(|e| Self::query_error(e, query))?;
                Ok(PostgresRows::new(None, stream, &self.labels))
            }
        }
    }
}

impl Executor for PostgresConnection {
    type Driver = PostgresDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<PostgresDriver>> {
        let sql = sql.as_str().trim_end().trim_end_matches(';');
        Ok(
            PostgresPrepared::new(self.client.prepare(&sql).await.map_err(|e| {
                let error = Error::new(e).context(format!(
                    "While preparing the query:\n{}",
                    truncate_long!(sql)
                ));
                log::error!("{error:#}");
                error
            })?)
            .into(),
        )
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        let mut query = query.as_query();
        let context = format!("While running the query:\n{}", query.as_mut());
        let mut owned = mem::take(query.as_mut());
        try_stream! {
            match owned {
                Query::Raw(raw) => {
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
                }
                Query::Prepared(..) => {
                    let mut transaction = self.begin().await?;
                    {
                        let mut stream = pin!(transaction.run(&mut owned));
                        while let Some(value) = stream.next().await.transpose()? {
                            yield match value {
                                QueryResult::Row(r) => QueryResult::Row(RowOwned::new(
                                    r.labels().into(),
                                    r.into_values().collect(),
                                )),
                                QueryResult::Affected(a) => QueryResult::Affected(a),
                            };
                        }
                    }
                    transaction.commit().await?;
                    *query.as_mut() = mem::take(&mut owned);
                }
            }
        }
        .map_err(move |e: Error| {
            let error = e.context(context.clone());
            log::error!("{error:#}");
            error
        })
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        let mut query = query.as_query();
        PostgresStream::new(async move { self.fetch_inner(query.as_mut()).await })
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
        let context = || {
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
            .with_context(context)
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
                .with_context(context)
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
        match writer.finish().await.with_context(context) {
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

impl Connection for PostgresConnection {
    async fn connect(driver: &PostgresDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to Postgres";
        let mut url = Self::sanitize_url(driver, url).context(context)?;
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
            let config = Self::tls_config(
                take_url_param("sslrootcert", "PGSSLROOTCERT", true),
                take_url_param("sslcert", "PGSSLCERT", true),
                take_url_param("sslkey", "PGSSLKEY", true),
            )?;
            if !url.query_pairs().any(|(key, _)| key == "sslmode") {
                let mode = match sslmode.as_str() {
                    "verify-ca" | "verify-full" => "require",
                    mode => mode,
                };
                url.query_pairs_mut().append_pair("sslmode", mode);
            }
            let connector = MakeRustlsConnect::new(config);
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
            labels: Default::default(),
        })
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
