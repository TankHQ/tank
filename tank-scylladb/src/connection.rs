use crate::{RowWrap, ScyllaDBDriver, ScyllaDBPrepared, ScyllaDBTransaction};
use async_stream::stream;
use scylla::{
    client::{PoolSize, session::Session, session_builder::SessionBuilder},
    frame::Compression,
    response::PagingState,
};
use std::{borrow::Cow, num::NonZeroUsize, ops::ControlFlow, pin::pin, sync::Arc, time::Duration};
use tank_core::{
    AsQuery, Connection, Driver, Error, ErrorContext, Executor, Query, QueryResult, Result,
    RowLabeled,
    stream::{Stream, StreamExt, TryStreamExt},
    truncate_long,
};
use url::Url;

pub struct ScyllaDBConnection {
    pub(crate) session: Session,
}

impl Executor for ScyllaDBConnection {
    type Driver = ScyllaDBDriver;

    fn driver(&self) -> &Self::Driver {
        &ScyllaDBDriver {}
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
        let context = || format!("While trying to connect to `{}`", url);
        let prefix = format!("{}://", <Self::Driver as Driver>::NAME);
        if !url.starts_with(&prefix) {
            let error = Error::msg(format!(
                "ScyllaDB connection url must start with `{}`",
                &prefix
            ))
            .context(context());
            log::error!("{:#}", error);
            return Err(error);
        }
        let url = Url::parse(&url).with_context(context)?;
        let hostname = url.host_str().with_context(context)?;
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
    async fn begin(&mut self) -> Result<ScyllaDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by ScyllaDB"))
    }
}
