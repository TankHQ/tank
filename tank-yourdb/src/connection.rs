use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, Result,
    stream::{self, Stream},
};

pub struct YourDBConnection {}

impl Executor for YourDBConnection {
    type Driver = YourDBDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<Self::Driver>> {
        // Return Err if not supported
        Ok(Query::Prepared(YourDBPrepared::new()))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        stream::iter([])
    }
}

impl Connection for YourDBConnection {
    async fn connect(url: Cow<'static, str>) -> Result<Self> {
        let context = || format!("While trying to connect to `{url}`");
        let url = Self::sanitize_url(url);
        // Establish connection
        Ok(YourDBConnection {})
    }

    async fn begin(&mut self) -> Result<YourDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
}
