use crate::{RowWrap, ScyllaDBDriver, ScyllaDBPrepared, ScyllaDBTransaction};
use anyhow::anyhow;
use async_stream::stream;
use rustls::{
    ClientConfig, RootCertStore,
    crypto::{CryptoProvider, aws_lc_rs::default_provider},
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use scylla::{
    client::{PoolSize, WriteCoalescingDelay, session::Session, session_builder::SessionBuilder},
    response::PagingState,
    statement::{
        Consistency,
        batch::{Batch, BatchType},
    },
};
use std::{
    borrow::Cow, cmp::min, net::IpAddr, num::NonZeroU64, ops::ControlFlow, pin::pin, str::FromStr,
    sync::Arc, time::Duration,
};
use tank_core::{
    AsQuery, AsValue, BinaryOp, BinaryOpType, ColumnRef, Connection, Error, ErrorContext, Executor,
    MAX_BATCH_SIZE, Query, QueryResult, RawQuery, Result, Row, Sql, extract_number,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};

/// Connection wrapper for ScyllaDB/Cassandra sessions.
///
/// Holds the underlying `scylla::Session` and exposes `Executor`/`Connection` implementations for the ScyllaDB driver.
#[derive(Debug)]
pub struct ScyllaDBConnection {
    pub(crate) session: Session,
    pub(crate) driver: ScyllaDBDriver,
    pub(crate) batch_size: usize,
}

pub type CassandraConnection = ScyllaDBConnection;

impl ScyllaDBConnection {
    /// Used when the server setting cannot be read.
    const DEFAULT_BATCH_SIZE: usize = 50 << 10;

    /// True when connected to Apache Cassandra rather than ScyllaDB.
    pub const fn is_cassandra(&self) -> bool {
        self.driver.is_cassandra
    }

    /// Reads the batch size limit from the server.
    async fn fetch_batch_size(&mut self) -> usize {
        let (schema, table, setting) = if self.is_cassandra() {
            ("system_views", "settings", "batch_size_fail_threshold")
        } else {
            ("system", "config", "batch_size_fail_threshold_in_kb")
        };
        let query = Sql()
            .select([ColumnRef::new("value")])
            .from((schema, table))
            .where_expr(BinaryOp {
                op: BinaryOpType::Equal,
                lhs: ColumnRef::new("name"),
                rhs: setting,
            })
            .build(&self.driver());
        let raw = pin!(self.fetch(query))
            .next()
            .await
            .and_then(Result::ok)
            .and_then(|r| r.value(0).and_then(|v| String::decode_value(v).ok()))
            .unwrap_or_default();
        // Leave headroom for row-size variance and cap giant batches.
        min(Self::parse_size(&raw) / 4 * 3, MAX_BATCH_SIZE)
    }

    /// Parses possibly quoted, value with memory unit
    fn parse_size(value: &str) -> usize {
        let mut value = value.trim().trim_matches('"');
        let number = extract_number::<false>(&mut value).parse::<usize>().ok();
        let unit = match value.trim() {
            "B" => Some(1),
            "" | "KiB" => Some(1 << 10),
            "MiB" => Some(1 << 20),
            "GiB" => Some(1 << 30),
            _ => None,
        };
        number
            .zip(unit)
            .and_then(|(n, u)| n.checked_mul(u))
            .unwrap_or_else(|| {
                let default = Self::DEFAULT_BATCH_SIZE;
                log::warn!("Could not read the server batch size limit, will use {default} bytes");
                default
            })
    }

    pub fn begin_logged_batch<'c>(&'c mut self) -> ScyllaDBTransaction<'c> {
        ScyllaDBTransaction {
            connection: self,
            batch: Batch::new(BatchType::Logged),
            params: Default::default(),
        }
    }

    pub fn begin_unlogged_batch<'c>(&'c mut self) -> ScyllaDBTransaction<'c> {
        ScyllaDBTransaction {
            connection: self,
            batch: Batch::new(BatchType::Unlogged),
            params: Default::default(),
        }
    }

    pub fn begin_counter_batch<'c>(&'c mut self) -> ScyllaDBTransaction<'c> {
        ScyllaDBTransaction {
            connection: self,
            batch: Batch::new(BatchType::Counter),
            params: Default::default(),
        }
    }

    /// Access the underlying session object from `scylla` crate
    pub const fn session_ref(&self) -> &Session {
        &self.session
    }

    /// Access the underlying session object from `scylla` crate
    pub const fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }
}

impl Executor for ScyllaDBConnection {
    type Driver = ScyllaDBDriver;

    fn driver(&self) -> Self::Driver {
        self.driver
    }

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    async fn do_prepare(&mut self, sql: &str) -> Result<ScyllaDBPrepared> {
        let context = format!("While preparing the query:\n{}", truncate_long(sql));
        let statement = self.session.prepare(sql).await.with_context(|| context)?;
        Ok(ScyllaDBPrepared::new(statement))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ScyllaDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        let mut query = query.as_query(&self.driver());
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        stream! {
            let mut paging_state = PagingState::start();
            loop {
                let (query_result, paging_state_response) = match query.as_mut() {
                    Query::Raw(RawQuery(sql)) => {
                        self.session
                            .query_single_page(sql.as_str(), &[], paging_state)
                            .await?
                    }
                    Query::Prepared(prepared) => {
                        let params = prepared.take_params()?;
                        self.session
                            .execute_single_page(&prepared.statement.clone(), params, paging_state)
                            .await?
                    }
                };
                if query_result.is_rows() {
                    for row in query_result.into_rows_result()?.rows::<RowWrap>()? {
                        let row = row?.0;
                        yield Ok(QueryResult::Row(row));
                    }
                } else {
                    // The driver does not give the number of affected rows
                    yield Ok(QueryResult::Affected(Default::default()));
                }
                match paging_state_response.into_paging_control_flow() {
                    ControlFlow::Break(..) => {
                        break;
                    }
                    ControlFlow::Continue(new_paging_state) => {
                        paging_state = new_paging_state;
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

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<ScyllaDBDriver> + 's,
    ) -> impl Stream<Item = Result<impl Row + 's>> + Send {
        let mut query = query.as_query(&self.driver());
        let context = Arc::new(format!("While fetching the query:\n{}", query.as_mut()));
        stream! {
            let stream = match query.as_mut() {
                Query::Raw(raw) => {
                    let sql = raw.0.as_str();
                    self.session
                        .query_iter(sql, [])
                        .await?
                        .rows_stream::<RowWrap>()?
                }
                Query::Prepared(prepared) => {
                    let params = prepared.take_params()?;
                    self.session
                        .execute_iter(prepared.statement.clone(), params)
                        .await?
                        .rows_stream::<RowWrap>()?
                }
            };
            let mut stream = pin!(stream);
            while let Some(row) = stream.next().await.transpose()? {
                yield Ok(row.0)
            }
        }
        .map_err(move |e: Error| {
            let error = e.context(context.clone());
            log::error!("{error:#}");
            error
        })
    }
}

impl Connection for ScyllaDBConnection {
    async fn connect(driver: &ScyllaDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let context = "While trying to connect to ScyllaDB";
        let hostname = url.host_str().context(context)?;
        let port = url.port();
        let username = url.username();
        let password = url.password();
        let address = if let Some(port) = port {
            Cow::Owned(format!("{hostname}:{port}"))
        } else {
            Cow::Borrowed(hostname)
        };
        let mut session = SessionBuilder::new().known_node(address);
        if !username.is_empty() {
            session = session.user(username, password.unwrap_or_default());
        }
        if let Some(mut segments) = url.path_segments() {
            if let Some(keyspace) = segments.next() {
                session = session.use_keyspace(keyspace, true);
            }
        }
        if CryptoProvider::get_default().is_none() {
            let _ = default_provider().install_default();
        }
        let mut root_store = RootCertStore::empty();
        let mut client_certs = Vec::new();
        let mut client_key = None;
        let mut ssl = false;
        let mut keyspaces = Vec::new();
        for (k, v) in url.query_pairs() {
            macro_rules! context_try {
                ($value:expr) => {
                    match $value {
                        Ok(v) => v,
                        Err(e) => {
                            let error = anyhow!("{e}")
                                .context(format!("URL param `{k} = {v}`"))
                                .context(context);
                            log::error!("{error:#}");
                            return Err(error);
                        }
                    }
                };
            }
            match k.as_ref() {
                "ssl_ca" => {
                    for cert in context_try!(CertificateDer::pem_file_iter(v.as_ref())) {
                        context_try!(root_store.add(context_try!(cert)));
                    }
                    ssl = true;
                }
                "ssl_cert" => {
                    for cert in context_try!(CertificateDer::pem_file_iter(v.as_ref())) {
                        client_certs.push(context_try!(cert));
                    }
                    ssl = true;
                }
                "ssl_key" => {
                    client_key = Some(context_try!(PrivateKeyDer::from_pem_file(v.as_ref())));
                    ssl = true;
                }
                "local_ip_address" => {
                    session =
                        session.local_ip_address(Some(context_try!(IpAddr::from_str(v.as_ref()))));
                }
                "compression" => {
                    session = session.compression(Some(context_try!(FromStr::from_str(&v))));
                }
                "schema_agreement_interval" => {
                    session = session.schema_agreement_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)),)
                    ));
                }
                "tcp_nodelay" => {
                    session = session.tcp_nodelay(context_try!(FromStr::from_str(&v)));
                }
                "tcp_keepalive_interval" => {
                    session = session.tcp_keepalive_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                "use_keyspace" => {
                    session = session.use_keyspace(v.as_ref(), true);
                }
                "connection_timeout" => {
                    session = session.connection_timeout(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)),)
                    ));
                }
                "pool_size_per_host" => {
                    session = session.pool_size(PoolSize::PerHost(context_try!(
                        context_try!(usize::from_str(&v)).try_into()
                    )));
                }
                "pool_size_per_shard" => {
                    session = session.pool_size(PoolSize::PerShard(context_try!(
                        context_try!(usize::from_str(&v)).try_into()
                    )));
                }
                "disallow_shard_aware_port" => {
                    session =
                        session.disallow_shard_aware_port(context_try!(FromStr::from_str(&v)));
                }
                "keyspaces_to_fetch" => {
                    keyspaces.push(v.into_owned());
                }
                "fetch_schema_metadata" => {
                    session = session.fetch_schema_metadata(context_try!(FromStr::from_str(&v)));
                }
                "metadata_request_serverside_timeout" => {
                    session = session.metadata_request_serverside_timeout(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                "keepalive_interval" => {
                    session = session.keepalive_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                "keepalive_timeout" => {
                    session = session.keepalive_timeout(context_try!(Duration::try_from_secs_f64(
                        context_try!(FromStr::from_str(&v))
                    )));
                }
                "schema_agreement_timeout" => {
                    session = session.schema_agreement_timeout(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                "auto_await_schema_agreement" => {
                    session =
                        session.auto_await_schema_agreement(context_try!(FromStr::from_str(&v)));
                }
                "hostname_resolution_timeout" => {
                    session = session.hostname_resolution_timeout(Some(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    )));
                }
                "refresh_metadata_on_auto_schema_agreement" => {
                    session = session.refresh_metadata_on_auto_schema_agreement(context_try!(
                        FromStr::from_str(&v)
                    ));
                }
                "tracing_info_fetch_attempts" => {
                    session =
                        session.tracing_info_fetch_attempts(context_try!(FromStr::from_str(&v)));
                }
                "tracing_info_fetch_interval" => {
                    session = session.tracing_info_fetch_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                "tracing_info_fetch_consistency" => {
                    session = session.tracing_info_fetch_consistency(context_try!(
                        Consistency::try_from(context_try!(u16::from_str(&v)))
                    ));
                }
                "write_coalescing_delay" => {
                    session = session.write_coalescing(true);
                    session = session.write_coalescing_delay(
                        if v.eq_ignore_ascii_case("SmallNondeterministic") {
                            WriteCoalescingDelay::SmallNondeterministic
                        } else if let Ok(v) = NonZeroU64::from_str(&v) {
                            WriteCoalescingDelay::Milliseconds(v)
                        } else {
                            return context_try!(Err(anyhow!(
                                "Unexpected value for write_coalescing_delay: `{v}`"
                            )));
                        },
                    );
                }
                "cluster_metadata_refresh_interval" => {
                    session = session.cluster_metadata_refresh_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                k => {
                    let error =
                        anyhow!("Unexpected parameter in connection url: `{k}`").context(context);
                    log::error!("{error:#}");
                    return Err(error);
                }
            }
        }

        if ssl {
            let builder = ClientConfig::builder().with_root_certificates(root_store);
            let config = match (client_certs.is_empty(), client_key) {
                (false, Some(key)) => builder
                    .with_client_auth_cert(client_certs, key)
                    .context(context)?,
                (true, None) => builder.with_no_client_auth(),
                (false, None) => {
                    let error = anyhow!("SSL KEY file must be provided when SSL CERT is specified")
                        .context(context);
                    log::error!("{error:#}");
                    return Err(error);
                }
                (true, Some(_)) => {
                    let error = anyhow!("SSL CERT file must be provided when SSL KEY is specified")
                        .context(context);
                    log::error!("{error:#}");
                    return Err(error);
                }
            };
            session = session.tls_context(Some(Arc::new(config)));
        }
        if !keyspaces.is_empty() {
            session = session.keyspaces_to_fetch(keyspaces);
        }

        let session = tokio::task::spawn(async move { session.build().await })
            .await
            .map_err(|e| anyhow!("ScyllaDB session build panicked: {e}"))?
            .map_err(Error::new)?;
        let mut connection = ScyllaDBConnection {
            session,
            driver: *driver,
            batch_size: 0,
        };
        // ScyllaDB nodes advertise shard metadata during the CQL handshake, Cassandra nodes do not.
        connection.driver.is_cassandra = !connection
            .session
            .get_cluster_state()
            .get_nodes_info()
            .iter()
            .any(|n| n.sharder().is_some());
        connection.batch_size = connection.fetch_batch_size().await;
        Ok(connection)
    }

    async fn begin<'c>(&'c mut self) -> Result<ScyllaDBTransaction<'c>> {
        Ok(Self::begin_logged_batch(self))
    }
}
