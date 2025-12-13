use crate::{MySQLDriver, MySQLQueryable, MySQLTransaction};
use mysql_async::{ClientIdentity, Conn, Opts, OptsBuilder};
use std::{borrow::Cow, env, path::PathBuf};
use tank_core::{
    Connection, Driver, Error, ErrorContext, Result, impl_executor_transaction, truncate_long,
};
use url::Url;

pub struct MySQLConnection {
    pub(crate) conn: MySQLQueryable<Conn>,
}

pub type MariaDBConnection = MySQLConnection;

impl_executor_transaction!(MySQLDriver, MySQLConnection, conn);

impl Connection for MySQLConnection {
    async fn connect(url: Cow<'static, str>) -> Result<MySQLConnection> {
        let context = || format!("While trying to connect to `{}`", truncate_long!(url));
        let prefix = format!("{}://", <Self::Driver as Driver>::NAME);
        if !url.starts_with(&prefix) {
            let error = Error::msg(format!(
                "MySQL connection url must start with `{}`",
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
        let ssl_ca = take_url_param("ssl_ca", "MYSQL_SSL_CA", true);
        let ssl_cert = take_url_param("ssl_cert", "MYSQL_SSL_CERT", true);
        let ssl_pass = take_url_param("ssl_pass", "MYSQL_SSL_PASS", true);
        let opts = Opts::from_url(url.as_str()).with_context(context)?;
        let mut ssl_opts = opts.ssl_opts().cloned();
        let mut opts = OptsBuilder::from_opts(opts);
        if let Some(ssl_ca) = ssl_ca {
            let ca_path = PathBuf::from(ssl_ca);
            if !ca_path.exists() {
                let error = Error::msg(format!(
                    "SSL CA file not found: `{}`",
                    ca_path.to_string_lossy()
                ))
                .context(context());
                log::error!("{:#}", error);
                return Err(error);
            }
            let certs = vec![ca_path.into()];
            ssl_opts = Some(ssl_opts.unwrap_or_default().with_root_certs(certs));
        }
        if let Some(ssl_cert) = ssl_cert {
            let ssl_cert = PathBuf::from(ssl_cert);
            if !ssl_cert.exists() {
                let error = Error::msg(format!(
                    "SSL CERT file not found: `{}`",
                    ssl_cert.to_string_lossy()
                ))
                .context(context());
                log::error!("{:#}", error);
                return Err(error);
            }
            let mut identity = ClientIdentity::new(ssl_cert.into());
            if let Some(ssl_pass) = ssl_pass {
                identity = identity.with_password(ssl_pass);
            };
            ssl_opts = Some(
                ssl_opts
                    .unwrap_or_default()
                    .with_client_identity(Some(identity)),
            );
        }
        opts = opts.ssl_opts(ssl_opts);
        let connection = Conn::new(opts).await.with_context(context)?;
        Ok(MySQLConnection {
            conn: MySQLQueryable {
                executor: connection,
            },
        })
    }

    #[allow(refining_impl_trait)]
    fn begin(&mut self) -> impl Future<Output = Result<MySQLTransaction<'_>>> {
        MySQLTransaction::new(self)
    }
}
