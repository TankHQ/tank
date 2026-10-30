use crate::{
    ChDBDriver, ChDBPrepared, ChDBSqlWriter, ChDBTransaction, native::NativeConnection, streaming,
};
use anyhow::anyhow;
use std::{borrow::Cow, mem, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, RawQuery, Result, Row,
    describe_path,
    stream::{self, Stream, TryStreamExt},
};
use tokio::{sync::Mutex, task::spawn_blocking};

/// Wrapper around chdb connection.
/// Provides helpers to execute queries and extract results into `tank_core` types.
#[derive(Debug)]
pub struct ChDBConnection {
    pub(crate) connection: Arc<Mutex<NativeConnection>>,
}

impl Connection for ChDBConnection {
    async fn connect(driver: &ChDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let make_context = || {
            format!(
                "While trying to connect to chDB {}",
                describe_path::<ChDBDriver>(&url)
            )
        };
        let path: Option<Cow<'static, str>> = url
            .query_pairs()
            .find_map(|(k, v)| (k.eq_ignore_ascii_case("path") && !v.is_empty()).then_some(v))
            .map(|v| Cow::Owned(v.to_string()))
            .or_else(|| {
                let raw = url.path().trim();
                (!raw.is_empty() && raw != "/")
                    .then(|| Cow::Owned(raw.trim_start_matches('/').to_string()))
            });
        let connection = spawn_blocking(move || -> Result<NativeConnection> {
            let mut connection = NativeConnection::open(path.as_deref())?;
            for sql in &[
                "SET allow_experimental_lightweight_delete=1",
                "SET join_use_nulls=1",
                "SET final=1",
                "SET output_format_json_quote_decimals=1",
            ] {
                connection
                    .query(sql, c"Null")
                    .map_err(|e| anyhow!("Failed to apply session setting '{sql}': {e}"))?;
            }
            Ok(connection)
        })
        .await
        .map_err(Error::new)
        .flatten()
        .with_context(make_context)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ChDBTransaction<'_>>> + Send {
        ChDBTransaction::new(self)
    }
}

impl Executor for ChDBConnection {
    type Driver = ChDBDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(
        &mut self,
        RawQuery { sql, params }: RawQuery,
    ) -> Result<Query<ChDBDriver>> {
        Ok(Query::Prepared(ChDBPrepared::new(sql, params)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ChDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        let mut query = query.as_query();
        stream::once(async move {
            let context = format!("While running the query:\n{}", query.as_mut());
            let connection = Arc::clone(&self.connection);
            let mut owned = mem::take(query.as_mut());
            let worker_context = context.clone();
            let join = spawn_blocking(move || {
                let result = (|| {
                    let (directive, sql) = match &mut owned {
                        Query::Raw(RawQuery { sql, .. }) => (None, Cow::Borrowed(sql.as_str())),
                        Query::Prepared(prepared) => {
                            let (directive, sql) =
                                ChDBSqlWriter::chdb().build_parameterized_sql(prepared)?;
                            prepared.take_params();
                            (directive, Cow::Owned(sql))
                        }
                    };
                    streaming::execute(connection, directive.as_deref(), &sql, worker_context)
                })();
                (owned, result)
            });
            let (owned, result) = join.await?;
            *query.as_mut() = owned;
            query.as_mut().clear_bindings().context(context.clone())?;
            result.map(stream::iter).map_err(|e| {
                let error = e.context(context);
                log::error!("{error:#}");
                error
            })
        })
        .try_flatten()
    }
}
