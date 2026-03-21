use crate::{CassandraDriver, RowWrap, ScyllaDBDriver, ScyllaDBPrepared, ScyllaDBTransaction};
use async_stream::stream;
use openssl::ssl::{SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use scylla::{
    client::{PoolSize, WriteCoalescingDelay, session::Session, session_builder::SessionBuilder},
    response::PagingState,
    statement::{
        Consistency,
        batch::{Batch, BatchType},
    },
};
use std::{
    borrow::Cow, net::IpAddr, num::NonZeroU64, ops::ControlFlow, pin::pin, str::FromStr, sync::Arc,
    time::Duration,
};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, RawQuery, Result, Row,
    impl_executor_transaction,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};

/// Connection wrapper for ScyllaDB/Cassandra sessions.
///
/// Holds the underlying `scylla::Session` and exposes `Executor`/`Connection` implementations for the ScyllaDB driver.
#[derive(Debug)]
pub struct ScyllaDBConnection {
    pub(crate) session: Session,
}

pub struct CassandraConnection {
    pub scylla: ScyllaDBConnection,
}

impl_executor_transaction!(CassandraDriver, CassandraConnection, scylla);

impl ScyllaDBConnection {
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
    pub fn session_ref(&self) -> &Session {
        &self.session
    }

    /// Access the underlying session object from `scylla` crate
    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }
}

impl Executor for ScyllaDBConnection {
    type Driver = ScyllaDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(&mut self, sql: String) -> Result<Query<ScyllaDBDriver>> {
        let context = format!("While preparing the query:\n{}", truncate_long!(sql));
        let statement = self.session.prepare(sql).await.with_context(|| context)?;
        Ok(Query::Prepared(ScyllaDBPrepared::new(statement)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ScyllaDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
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
            log::error!("{:#}", error);
            error
        })
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<ScyllaDBDriver> + 's,
    ) -> impl Stream<Item = Result<Row>> + Send {
        let mut query = query.as_query();
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
            log::error!("{:#}", error);
            error
        })
    }
}

impl Connection for ScyllaDBConnection {
    async fn connect(driver: &ScyllaDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = format!("While trying to connect to `{}`", url);
        let url = Self::sanitize_url(driver, url)?;
        let hostname = url.host_str().with_context(|| context.clone())?;
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
        let mut context_builder =
            SslContextBuilder::new(SslMethod::tls()).with_context(|| context.clone())?;
        context_builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        let mut ssl = false;
        let mut keyspaces = Vec::new();
        for (k, v) in url.query_pairs() {
            macro_rules! context_try {
                ($value:expr) => {
                    match $value {
                        Ok(v) => v,
                        Err(e) => {
                            let e = Error::msg(format!("{e}"))
                                .context(format!("URL param `{k} = {v}`"))
                                .context(context.clone());
                            log::error!("{e:#}");
                            return Err(e);
                        }
                    }
                };
            }
            match k.as_ref() {
                "ssl_ca" => {
                    context_try!(context_builder.set_ca_file(v.as_ref()));
                    ssl = true;
                }
                "ssl_cert" => {
                    context_try!(
                        context_builder.set_certificate_file(v.as_ref(), SslFiletype::PEM)
                    );
                    ssl = true;
                }
                "ssl_key" => {
                    context_try!(
                        context_builder.set_private_key_file(v.as_ref(), SslFiletype::PEM)
                    );
                    context_try!(context_builder.check_private_key());
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
                            return context_try!(Err(Error::msg(format!(
                                "Unexpected value for write_coalescing_delay: `{v}`"
                            ))));
                        },
                    );
                }
                "cluster_metadata_refresh_interval" => {
                    session = session.cluster_metadata_refresh_interval(context_try!(
                        Duration::try_from_secs_f64(context_try!(FromStr::from_str(&v)))
                    ));
                }
                k => {
                    let e = Error::msg(format!("Unexpected parameter in connection url: `{k}`"))
                        .context(context);
                    log::error!("{e:#}");
                    return Err(e);
                }
            }
        }

        if ssl {
            session = session.tls_context(Some(context_builder.build()));
        }
        if !keyspaces.is_empty() {
            session = session.keyspaces_to_fetch(keyspaces);
        }

        Ok(ScyllaDBConnection {
            session: session.build().await?,
        })
    }

    async fn begin<'c>(&'c mut self) -> Result<ScyllaDBTransaction<'c>> {
        Ok(Self::begin_logged_batch(self))
    }
}
