use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, RawQuery, Result, Row,
    stream::{self, Stream},
};
use url::Url;

#[derive(Debug)]
pub struct YourDBConnection {
    pub(crate) url: Url,
}

impl Connection for YourDBConnection {
    async fn connect(driver: &YourDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let url = Self::sanitize_url(driver, url)?;
        // Establish connection
        Ok(YourDBConnection { url })
    }

    async fn begin(&mut self) -> Result<YourDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
}

impl Executor for YourDBConnection {
    type Driver = YourDBDriver;

    async fn do_prepare(&mut self, query: RawQuery) -> Result<Query<YourDBDriver>> {
        // Return Err if not supported
        Ok(Query::Prepared(YourDBPrepared::new(query)))
    }

    fn run<'s>(
        &'s mut self,
        _query: impl AsQuery<YourDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        stream::empty::<Result<QueryResult>>()
    }
}
