use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, Result,
    stream::{self, Stream},
};

pub struct YourDBConnection {}

impl Executor for YourDBConnection {
    type Driver = YourDBDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<YourDBDriver>> {
        // Return Err if not supported
        Ok(Query::Prepared(YourDBPrepared::new()))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<YourDBDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        stream::iter([])
    }
}

impl Connection for YourDBConnection {
    async fn connect(driver: &YourDBDriver, url: Cow<'static, str>) -> Result<Self> {
        let context = "While trying to connect to YourDB";
        let url = Self::sanitize_url(driver, url);
        // Establish connection
        Ok(YourDBConnection {})
    }

    async fn begin(&mut self) -> Result<YourDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
}
