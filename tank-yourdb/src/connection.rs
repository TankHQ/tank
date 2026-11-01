use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, ErrorContext, Executor, Query, QueryResult, Result, Row, RowOwned, anyhow,
    stream::{self, Stream},
};
use url::Url;

#[derive(Debug)]
pub struct YourDBConnection {
    pub(crate) url: Url,
}

impl Executor for YourDBConnection {
    type Driver = YourDBDriver;

    async fn do_prepare(&mut self, _sql: String) -> Result<Query<YourDBDriver>> {
        // Return Err if not supported
        Ok(Query::Prepared(YourDBPrepared::new()))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<YourDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        stream::empty::<Result<QueryResult<RowOwned>>>()
    }
}

impl Connection for YourDBConnection {
    async fn connect(driver: &YourDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to YourDB";
        let url = Self::sanitize_url(driver, url).context(context)?;
        // Establish connection
        Ok(YourDBConnection { url })
    }

    async fn begin(&mut self) -> Result<YourDBTransaction<'_>> {
        Err(anyhow!("Transactions are not supported by YourDB"))
    }
}
