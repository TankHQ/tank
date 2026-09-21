use crate::{
    ClickHouseDriver, ClickHousePrepared, ClickHouseSqlWriter, ClickHouseTransaction, extract_value,
};
use anyhow::{Error, anyhow};
use async_stream::try_stream;
use futures::{StreamExt, TryStreamExt};
use klickhouse::{Client, ClientOptions};
use std::{borrow::Cow, fmt, str::FromStr, sync::Arc};
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, RawQuery, Result, Row,
    stream::Stream, truncate_long,
};

/// ClickHouse connection wrapper.
///
/// Holds the underlying `klickhouse::Client` and implements the tank_core::Connection`/`Executor` APIs
pub struct ClickHouseConnection {
    pub(crate) client: Client,
}

impl Connection for ClickHouseConnection {
    async fn connect(driver: &ClickHouseDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        let hostname = url.host_str().context("No hostname")?;
        let port = url.port();
        let username = url.username();
        let password = url.password();
        let address = if let Some(port) = port {
            Cow::Owned(format!("{hostname}:{port}"))
        } else {
            Cow::Borrowed(hostname)
        };
        let context = format!(
            "While trying to connect to ClickHouse {}",
            truncate_long!(address)
        );
        let mut options = ClientOptions::default();
        if !username.is_empty() {
            options.username = username.into();
        };
        if let Some(password) = password
            && !password.is_empty()
        {
            options.password = password.into()
        };
        if let Some(database) = url.path_segments().and_then(|mut v| v.next())
            && !database.is_empty()
        {
            options.default_database = database.into();
        }
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
                "tcp_nodelay" => {
                    options.tcp_nodelay = context_try!(FromStr::from_str(&v));
                }
                k => {
                    let error = anyhow!("Unknown parameter in connection url: `{k}`")
                        .context(context.clone());
                    log::error!("{error:#}");
                    return Err(error);
                }
            }
        }
        let client = Client::connect(&address as &str, options)
            .await
            .map_err(Error::new)
            .with_context(|| context.clone())?;
        for sql in &[
            "SET allow_experimental_lightweight_delete=1",
            "SET join_use_nulls=1",
            "SET final=1",
        ] {
            client
                .execute(*sql)
                .await
                .map_err(|e| Error::new(e).context(format!("While executing: `{sql}`")))
                .with_context(|| context.clone())?;
        }
        Ok(ClickHouseConnection { client })
    }

    fn begin(&mut self) -> impl Future<Output = Result<ClickHouseTransaction<'_>>> + Send {
        ClickHouseTransaction::new(self)
    }
}

impl Executor for ClickHouseConnection {
    type Driver = ClickHouseDriver;

    fn accepts_multiple_statements(&self) -> bool {
        false
    }

    async fn do_prepare(&mut self, sql: String) -> Result<Query<ClickHouseDriver>> {
        Ok(Query::Prepared(ClickHousePrepared::new(sql)))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<ClickHouseDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let context = format!("While running the query:\n{}", query.as_mut());
        let client = self.client.clone();

        try_stream! {
            let sql = match query.as_mut() {
                Query::Raw(RawQuery(sql)) => Cow::Borrowed(sql.as_str()),
                Query::Prepared(prepared) => {
                    let sql = prepared
                        .build_sql(&ClickHouseSqlWriter::new())
                        .with_context(|| context.clone())?;
                    prepared.take_params();
                    Cow::Owned(sql)
                }
            };
            let mut stream = client
                .query_raw(sql.as_ref())
                .await
                .map_err(Error::new)
                .with_context(|| context.clone())?;
            let mut got_rows = false;
            while let Some(block) = stream
                .next()
                .await
                .transpose()
                .map_err(Error::new)
                .with_context(|| context.clone())?
            {
                if block.column_types.is_empty() {
                    continue;
                }
                got_rows = true;
                if block.rows == 0 {
                    continue;
                }
                let names: Arc<[String]> = block
                    .column_types
                    .keys()
                    .map(|n| n.rsplit('.').next().unwrap_or(n).to_owned())
                    .collect::<Vec<_>>()
                    .into();
                let columns: Vec<(&klickhouse::Type, &Vec<klickhouse::Value>)> = block
                    .column_types
                    .values()
                    .zip(block.column_data.values())
                    .collect();
                for row_idx in 0..block.rows as usize {
                    let values = columns
                        .iter()
                        .map(|(ty, column)| {
                            extract_value(ty, column[row_idx].clone())
                                .with_context(|| context.clone())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    yield QueryResult::Row(Row::new(names.clone(), values.into()));
                }
            }
            if !got_rows {
                yield QueryResult::Affected(Default::default());
            }
        }
        .map_err(move |e| {
            log::error!("{e:#}");
            e
        })
    }
}

impl fmt::Debug for ClickHouseConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConnection").finish()
    }
}
