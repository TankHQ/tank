use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, Executor, OwnedResults, Query, RawQuery, Result,
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

    fn run<'s, Q>(
        &'s mut self,
        _query: Q,
    ) -> impl Future<Output = Result<<Self::Driver as tank_core::Driver>::Results<'s, Q::Output>>>
    + Send
    where
        Q: AsQuery<YourDBDriver> + Send + 's,
    {
        async { Ok(OwnedResults::new(Vec::new())) }
    }
}
