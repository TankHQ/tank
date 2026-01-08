use crate::{CassandraDriver, RowWrap, ScyllaDBDriver, ScyllaDBPrepared, ScyllaDBTransaction};
use async_stream::stream;
use openssl::ssl::{SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use scylla::{
    client::{PoolSize, session::Session, session_builder::SessionBuilder},
    frame::Compression,
    response::PagingState,
    statement::batch::{Batch, BatchType},
};
use std::{borrow::Cow, num::NonZeroUsize, ops::ControlFlow, pin::pin, sync::Arc, time::Duration};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, Result, RowLabeled,
    impl_executor_transaction,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};

/// Connection wrapper for ScyllaDB/Cassandra sessions.
///
/// Holds the underlying `scylla::Session` and exposes `Executor`/`Connection` implementations for the ScyllaDB driver.
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
}

impl Executor for ScyllaDBConnection {
    type Driver = ScyllaDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn prepare(&mut self, sql: String) -> Result<Query<Self::Driver>> {
        let context = format!(
            "While preparing the query:\n{}",
            truncate_long!(sql.as_str())
        );
        let statement = self.session.prepare(sql).await.with_context(|| context)?;
        Ok(Query::Prepared(ScyllaDBPrepared::new(statement)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        stream! {
            let mut paging_state = PagingState::start();
            loop {
                let (query_result, paging_state_response) = match query.as_mut() {
                    Query::Raw(sql) => {
                        let sql = sql.as_str();
                        self.session
                            .query_single_page(sql, &[], paging_state)
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
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<RowLabeled>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While fetching the query:\n{}", query.as_mut()));
        stream! {
            let stream = match query.as_mut() {
                Query::Raw(sql) => {
                    let sql = sql.as_str();
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
    async fn connect(url: Cow<'static, str>) -> Result<ScyllaDBConnection> {
        let context = format!("While trying to connect to `{}`", url);
        let url = Self::sanitize_url(url)?;
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
        if let Some(mut segments) = url.path_segments()
            && let Some(keyspace) = segments.next()
        {
            session = session.use_keyspace(keyspace, true);
        }
        let mut context_builder =
            SslContextBuilder::new(SslMethod::tls()).with_context(|| context.clone())?;
        context_builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        let mut ssl = false;
        if let Some(path) = &url
            .query_pairs()
            .find_map(|(k, v)| if k == "sslca" { Some(v) } else { None })
        {
            context_builder
                .set_ca_file(match path {
                    Cow::Borrowed(v) => v,
                    Cow::Owned(v) => v.as_str(),
                })
                .with_context(|| context.clone())?;
            ssl = true;
        };
        if let Some(path) = &url
            .query_pairs()
            .find_map(|(k, v)| if k == "sslcert" { Some(v) } else { None })
        {
            context_builder
                .set_certificate_file(
                    match path {
                        Cow::Borrowed(v) => v,
                        Cow::Owned(v) => v.as_str(),
                    },
                    SslFiletype::PEM,
                )
                .with_context(|| context.clone())?;
            ssl = true;
        };
        if let Some(path) = &url
            .query_pairs()
            .find_map(|(k, v)| if k == "sslkey" { Some(v) } else { None })
        {
            context_builder
                .set_private_key_file(
                    match path {
                        Cow::Borrowed(v) => v,
                        Cow::Owned(v) => v.as_str(),
                    },
                    SslFiletype::PEM,
                )
                .with_context(|| context.clone())?;
            context_builder
                .check_private_key()
                .with_context(|| context.clone())?;
            ssl = true;
        };
        if ssl {
            session = session.tls_context(Some(context_builder.build()));
        }
        if let Some(compression) = url.query_pairs().find_map(|(k, v)| {
            if k == "compression"
                && let Some(value) = match &*v {
                    "Lz4" => Some(Compression::Lz4),
                    "Snappy" => Some(Compression::Snappy),
                    _ => {
                        log::error!("Invalid value for `compression`, expected: `Lz4`, `Snappy`");
                        None
                    }
                }
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.compression(compression.into());
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if k == "schema_agreement_interval"
                && let Ok(value) = str::parse::<f64>(&*v)
                && let Ok(value) = Duration::try_from_secs_f64(value)
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.schema_agreement_interval(value);
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if k == "tcp_nodelay"
                && let Ok(value) = str::parse::<bool>(&*v)
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.tcp_nodelay(value);
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if k == "tcp_keepalive_interval"
                && let Ok(value) = str::parse::<f64>(&*v)
                && let Ok(value) = Duration::try_from_secs_f64(value)
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.tcp_keepalive_interval(value);
        };
        if let Some(value) = url
            .query_pairs()
            .find_map(|(k, v)| if k == "use_keyspace" { Some(v) } else { None })
        {
            session = session.use_keyspace(&*value, true);
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if k == "connection_timeout"
                && let Ok(value) = str::parse::<f64>(&*v)
                && let Ok(value) = Duration::try_from_secs_f64(value)
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.connection_timeout(value);
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if (k == "pool_size_per_host" || k == "pool_size_per_shard")
                && let Ok(value) = str::parse::<usize>(&*v)
            {
                NonZeroUsize::new(value).map(|v| {
                    if k == "pool_size_per_host" {
                        PoolSize::PerHost(v)
                    } else {
                        PoolSize::PerShard(v)
                    }
                })
            } else {
                None
            }
        }) {
            session = session.pool_size(value);
        };
        if let Some(value) = url.query_pairs().find_map(|(k, v)| {
            if k == "disallow_shard_aware_port"
                && let Ok(value) = str::parse::<bool>(&*v)
            {
                Some(value)
            } else {
                None
            }
        }) {
            session = session.disallow_shard_aware_port(value);
        };
        Ok(ScyllaDBConnection {
            session: session.build().await?,
        })
    }

    #[allow(refining_impl_trait)]
    async fn begin<'c>(&'c mut self) -> Result<ScyllaDBTransaction<'c>> {
        Ok(Self::begin_logged_batch(self))
    }
}
