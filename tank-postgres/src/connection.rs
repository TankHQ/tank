use crate::{
    PostgresDriver, PostgresPrepared, PostgresTransaction, TankParam,
    rows::{PostgresResults, PostgresScratch},
    stream::ConnectionStream,
};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    future::Future,
    mem,
    str::FromStr,
};
use tank_core::{AsQuery, Connection, ErrorContext, Executor, Query, RawQuery, Result, describe_url};
use url::Url;
use wtx::{
    database::DbClient as _,
    database::client::postgres::{ClientBuffer, Config, PostgresClient},
    net::Uri,
    rng::{ChaCha20, CryptoSeedableRng as _},
    tls::{PlaintextCtx, TlsConfig, TlsConnector},
};

/// Concrete `wtx` client type used by this driver.
pub(crate) type WtxClient = PostgresClient<wtx::Error, ConnectionStream, PlaintextCtx>;

/// PostgreSQL connection backed by `wtx`.
pub struct PostgresConnection {
    pub(crate) client: WtxClient,
    /// Reusable decode buffers, cleared and refilled by every query so that a
    /// steady-state workload performs no per-query allocation.
    pub(crate) scratch: PostgresScratch,
}

impl Debug for PostgresConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnection").finish()
    }
}

impl Connection for PostgresConnection {
    async fn connect(driver: &PostgresDriver, url: Cow<'static, str>) -> Result<Self> {
        let mut url = Self::sanitize_url(driver, url)?;
        let make_context = |url: &Url| {
            format!(
                "While trying to connect to Postgres {}",
                describe_url::<PostgresDriver>(url)
            )
        };
        let sslmode = url
            .query_pairs()
            .find_map(|(k, v)| (k == "sslmode").then(|| v.to_string()))
            .or_else(|| std::env::var("PGSSLMODE").ok())
            .unwrap_or_else(|| "disable".into());
        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5432);
        let stream = if sslmode == "disable" {
            ConnectionStream::connect_plain(&host, port)
                .await
                .with_context(|| make_context(&url))?
        } else {
            #[cfg(feature = "tls")]
            {
                let connector = build_tls_connector(&mut url)?;
                ConnectionStream::connect_tls(&host, port, connector, &host)
                    .await
                    .with_context(|| make_context(&url))?
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(tank_core::anyhow!(
                    "TLS is not enabled for this build of tank-postgres (sslmode={sslmode})"
                )
                .context(make_context(&url)));
            }
        };
        let mut rng = ChaCha20::from_std_random().map_err(tank_core::Error::new)?;
        let client_buffer = ClientBuffer::new(usize::MAX, &mut rng);
        // The URI is rebuilt without tank-specific query params that wtx would
        // reject.
        let wtx_uri = sanitize_wtx_uri(&url);
        let uri = Uri::new(wtx_uri.as_str());
        let config = Config::from_uri(&uri).map_err(tank_core::Error::new)?;
        let tls_connector = TlsConnector::new(TlsConfig::plaintext(), rng, stream, uri);
        let client = PostgresClient::connect(client_buffer, &config, tls_connector)
            .await
            .map_err(tank_core::Error::new)
            .with_context(|| make_context(&url))?;
        Ok(Self {
            client,
            scratch: Default::default(),
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<PostgresTransaction<'_>>> + Send {
        PostgresTransaction::new(self)
    }
}

impl Executor for PostgresConnection {
    type Driver = PostgresDriver;

    /// Postgres' extended protocol rejects multi-command strings, so tank
    /// issues each statement separately (see `Entity::create_table`).
    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(
        &mut self,
        RawQuery { sql, .. }: RawQuery,
    ) -> Result<Query<PostgresDriver>> {
        let sql = sql.trim_end().trim_end_matches(';').to_string();
        // `wtx` prepares lazily on first execution, keyed by the statement hash,
        // so a prepared handle only needs to carry the SQL and the parameter
        // types (which are discovered on first run).
        Ok(PostgresPrepared::new(sql).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<PostgresDriver> + Send + 's,
    ) -> impl Future<Output = Result<impl tank_core::Results>> + Send {
        async move {
            let mut query = query.as_query();
            let owned = mem::take(query.as_mut());
            let result = match owned {
                Query::Raw(raw) => {
                    let r = self.run_prepared(&raw.sql, &[]).await;
                    *query.as_mut() = Query::Raw(raw);
                    r
                }
                Query::Prepared(mut prepared) => {
                    let params = prepared.take_params()?;
                    let r = self.run_prepared(&prepared.sql, &params).await;
                    *query.as_mut() = Query::Prepared(prepared);
                    r
                }
            };
            result.map_err(|e| {
                // Built lazily: formatting the query on the happy path would
                // allocate on every successful run.
                let error = e.context(format!("While running the query:\n{}", query.as_mut()));
                log::error!("{error:#}");
                error
            })
        }
    }
}

impl PostgresConnection {
    /// Runs one statement through the typed protocol, decoding with full type
    /// information.
    ///
    /// The returned view borrows both the client (whose read buffer holds the
    /// row bytes) and the scratch (labels and decoders), and decodes each value
    /// lazily.
    async fn run_prepared<'s>(
        &'s mut self,
        sql: &str,
        params: &[TankParam],
    ) -> Result<PostgresResults<'s>> {
        let stmt = crate::TankStmt::new(sql, params);
        let PostgresConnection { client, scratch } = self;
        let records = client
            .execute_stmt_many(stmt, params, |_| Ok(()))
            .await
            .map_err(tank_core::Error::new)?;
        scratch.prepare(&records)?;
        Ok(PostgresResults::new(
            &records,
            scratch.labels(),
            scratch.decoders(),
        ))
    }
}

/// Rebuilds a wtx-acceptable URI: wtx only understands `application_name` and
/// `channel_binding` query params, so tank-specific ones (`sslmode`, cert
/// paths, …) are stripped.
fn sanitize_wtx_uri(url: &Url) -> String {
    let mut url = url.clone();
    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k == "application_name" || k == "channel_binding")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.set_query(None);
    if !kept.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (k, v) in kept {
            pairs.append_pair(&k, &v);
        }
    }
    url.to_string()
}

/// Builds a `native-tls` connector from the URL, including client certificates
/// for mutual TLS.
#[cfg(feature = "tls")]
fn build_tls_connector(url: &mut Url) -> Result<tokio_native_tls::TlsConnector> {
    use native_tls::{Certificate, Identity, TlsConnector};
    let take = |url: &Url, key: &str, env_var: &str| -> Option<String> {
        url.query_pairs()
            .find_map(|(k, v)| (k == key).then(|| v.to_string()))
            .or_else(|| std::env::var(env_var).ok())
    };
    let root = take(url, "sslrootcert", "PGSSLROOTCERT")
        .unwrap_or_else(|| "~/.postgresql/root.crt".into());
    let cert =
        take(url, "sslcert", "PGSSLCERT").unwrap_or_else(|| "~/.postgresql/postgresql.crt".into());
    let key =
        take(url, "sslkey", "PGSSLKEY").unwrap_or_else(|| "~/.postgresql/postgresql.key".into());
    let mut builder = TlsConnector::builder();
    let root_path = expand_home(&root);
    if root_path.exists() {
        let pem = std::fs::read(&root_path)?;
        builder.add_root_certificate(Certificate::from_pem(&pem)?);
    }
    let cert_path = expand_home(&cert);
    let key_path = expand_home(&key);
    if cert_path.exists() && key_path.exists() {
        let cert_pem = std::fs::read(&cert_path)?;
        let key_pem = std::fs::read(&key_path)?;
        builder.identity(Identity::from_pkcs8(&cert_pem, &key_pem)?);
    }
    // Mutual TLS requires the client to trust the server and to verify it.
    builder.danger_accept_invalid_hostnames(false);
    Ok(tokio_native_tls::TlsConnector::from(builder.build()?))
}

#[cfg(feature = "tls")]
fn expand_home(path: &str) -> std::path::PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return std::path::PathBuf::from_str(&home).unwrap().join(rest);
    }
    std::path::PathBuf::from_str(path).unwrap()
}
