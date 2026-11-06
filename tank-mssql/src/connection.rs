use crate::{MSSQLDriver, MSSQLPrepared, MSSQLTransaction, ValueWrap};
use anyhow::anyhow;
use async_stream::try_stream;
use std::{
    borrow::Cow,
    env,
    fmt::{self, Debug, Formatter},
    path::PathBuf,
    sync::Arc,
};
use tank_core::{
    AsQuery, Connection, DynQuery, Error, ErrorContext, Executor, Query, QueryResult, RawQuery,
    Result, Row, RowLabels, RowsAffected, describe_url,
    stream::{Stream, TryStreamExt},
    write_escaped,
};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Query as TiberiusQuery, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use url::Url;

/// Connection wrapper for Microsoft SQL Server / Azure SQL.
pub struct MSSQLConnection {
    pub(crate) client: Client<Compat<TcpStream>>,
}

impl MSSQLConnection {
    fn percent_decode(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%'
                && i + 2 < bytes.len()
                && let (Some(hi), Some(lo)) = (
                    (bytes[i + 1] as char).to_digit(16),
                    (bytes[i + 2] as char).to_digit(16),
                )
            {
                out.push(((hi << 4) | lo) as u8);
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap_or_else(|_| input.to_owned())
    }

    async fn connect_client(config: Config) -> Result<Client<Compat<TcpStream>>> {
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Ok(Client::connect(config, tcp.compat_write()).await?)
    }
}

impl Connection for MSSQLConnection {
    async fn connect(driver: &MSSQLDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = |url: &Url| {
            format!(
                "While trying to connect to SQL Server {}",
                describe_url::<MSSQLDriver>(url)
            )
        };
        let mut config = Config::new();
        if let Some(host) = url.host_str() {
            config.host(host);
        }
        if let Some(port) = url.port() {
            config.port(port);
        }
        let user = Self::percent_decode(url.username());
        let password = url.password().map(Self::percent_decode).unwrap_or_default();
        if !user.is_empty() || !password.is_empty() {
            config.authentication(AuthMethod::sql_server(user, password));
        }
        let database = url
            .path_segments()
            .and_then(|mut s| s.find(|seg| !seg.is_empty()))
            .map(Self::percent_decode);
        let mut encryption = EncryptionLevel::Off;
        let mut trust_cert = false;
        let mut ssl_ca: Option<String> = None;
        for (key, value) in url.query_pairs() {
            match key.to_ascii_lowercase().as_str() {
                "trustservercertificate" | "trust_cert" => {
                    trust_cert =
                        matches!(value.to_ascii_lowercase().as_str(), "true" | "yes" | "1");
                }
                "trustservercertificateca" | "ssl_ca" | "sslrootcert" => {
                    ssl_ca = Some(value.into_owned());
                }
                "encrypt" | "require_ssl" | "sslmode" => {
                    encryption = match value.to_ascii_lowercase().as_str() {
                        "true" | "yes" | "1" | "required" | "require" | "prefer" => {
                            EncryptionLevel::Required
                        }
                        "false" | "no" | "0" | "off" | "disable" => EncryptionLevel::Off,
                        "not_supported" | "danger_plaintext" => EncryptionLevel::NotSupported,
                        _ => {
                            let error = anyhow!("Unexpected value `{value}` for `{key}`")
                                .context(make_context(&url));
                            log::error!("{error:#}");
                            return Err(error);
                        }
                    };
                }
                "applicationname" | "application name" | "application_name" => {
                    config.application_name(value);
                }
                "instance" | "instance_name" => {
                    config.instance_name(value);
                }
                "readonly" => {
                    config.readonly(matches!(
                        value.to_ascii_lowercase().as_str(),
                        "true" | "yes" | "1"
                    ));
                }
                _ => {}
            }
        }
        config.encryption(encryption);
        if ssl_ca.is_none() {
            ssl_ca = env::var("MSSQL_SSL_CA").ok();
        }
        if let Some(ca) = ssl_ca {
            let ca_path = PathBuf::from(&ca);
            if !ca_path.exists() {
                let error = anyhow!("SSL CA file not found: `{}`", ca_path.to_string_lossy())
                    .context(make_context(&url));
                log::error!("{error:#}");
                return Err(error);
            }
            config.trust_cert_ca(ca);
        } else if trust_cert || encryption == EncryptionLevel::Off {
            // TDS encrypts the login packet even when `EncryptionLevel::Off` is used.
            config.trust_cert();
        }
        if let Some(ref db) = database {
            config.database(db);
        }
        let client = match Self::connect_client(config.clone()).await {
            Ok(client) => client,
            Err(_first_err)
                if database
                    .as_deref()
                    .is_some_and(|db| !db.eq_ignore_ascii_case("master")) =>
            {
                let db = database.as_deref().unwrap();
                let mut master_config = config.clone();
                master_config.database("master");
                let mut master_client = Self::connect_client(master_config)
                    .await
                    .with_context(|| make_context(&url))?;
                let mut db_lit = DynQuery::default();
                write_escaped(&mut db_lit, db, '\'', "''");
                let mut db_ident = DynQuery::default();
                write_escaped(&mut db_ident, db, '"', "\"\"");
                let mut db_ident_esc = DynQuery::default();
                write_escaped(&mut db_ident_esc, &db_ident.as_str(), '\'', "''");
                let create_sql = format!(
                    "IF DB_ID(N'{}') IS NULL EXEC(N'CREATE DATABASE \"{}\"');",
                    db_lit.as_str(),
                    db_ident_esc.as_str(),
                );
                let _ = master_client
                    .simple_query(create_sql)
                    .await
                    .with_context(|| make_context(&url))?;
                let _ = master_client.close().await;
                Self::connect_client(config)
                    .await
                    .with_context(|| make_context(&url))?
            }
            Err(e) => return Err(e.context(make_context(&url))),
        };
        Ok(Self { client })
    }

    fn begin(&mut self) -> impl Future<Output = Result<MSSQLTransaction<'_>>> + Send {
        MSSQLTransaction::new(self)
    }

    async fn disconnect(self) -> Result<()> {
        self.client
            .close()
            .await
            .context("While disconnecting from SQL Server")
    }
}

impl Executor for MSSQLConnection {
    type Driver = MSSQLDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<MSSQLDriver>> {
        Ok(Query::Prepared(MSSQLPrepared::new(sql)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<MSSQLDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        try_stream! {
            let mut stream = match query.as_mut() {
                Query::Raw(RawQuery(sql)) => {
                    self.client.simple_query(sql.as_str()).await?
                }
                Query::Prepared(prepared) => {
                    let params = prepared.take_params()?;
                    let mut tiberius_query = TiberiusQuery::new(prepared.sql.as_str());
                    for param in params {
                        tiberius_query.bind(param);
                    }
                    tiberius_query.query(&mut self.client).await?
                }
            };
            let mut labels: Option<RowLabels> = None;
            while let Some(item) = stream.try_next().await? {
                match item {
                    QueryItem::Metadata(meta) => {
                        labels = Some(meta.columns().iter().map(|c| c.name().to_owned()).collect());
                    }
                    QueryItem::Row(row) => {
                        yield QueryResult::Row(ValueWrap::from_row(row, labels.as_ref())?);
                    }
                    QueryItem::RowsAffected(rows) => {
                        yield QueryResult::Affected(RowsAffected {
                            rows_affected: Some(rows),
                            last_affected_id: None,
                        });
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

impl Debug for MSSQLConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("MSSQLConnection")
    }
}
