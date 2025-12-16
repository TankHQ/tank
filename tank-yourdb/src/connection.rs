use crate::{YourDBDriver, YourDBPrepared, YourDBTransaction};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Driver, Error, Executor, Query, QueryResult, Result,
    stream::{self, Stream},
};

pub struct YourDBConnection {}

impl Executor for YourDBConnection {
    type Driver = YourDBDriver;

    fn driver(&self) -> &Self::Driver {
        &YourDBDriver {}
    }
    async fn prepare(&mut self, query: String) -> Result<Query<Self::Driver>> {
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
    async fn connect(url: Cow<'static, str>) -> Result<YourDBConnection> {
        let context = || format!("While trying to connect to `{}`", url);
        let prefix = format!("{}://", <Self::Driver as Driver>::NAME);
        if !url.starts_with(&prefix) {
            let error = Error::msg(format!(
                "YourDB connection url must start with `{}`",
                &prefix
            ))
            .context(context());
            log::error!("{:#}", error);
            return Err(error);
        }
        Ok(YourDBConnection {})
    }
    #[allow(refining_impl_trait)]
    async fn begin(&mut self) -> Result<YourDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by YourDB"))
    }
}
