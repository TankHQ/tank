use crate::{MongoDBDriver, MongoDBPrepared, MongoDBTransaction};
use async_stream::try_stream;
use mongodb::{Client, Database, bson, options::ClientOptions};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, Result, RowLabeled,
    RowsAffected, Value as TankValue,
    stream::{self, Stream},
    truncate_long,
};

/// Minimal MongoDB connection wrapper used by the driver.
pub struct MongoDBConnection {
    client: Client,
    database: Database,
}

impl MongoDBConnection {
    pub fn new(client: Client, database: Database) -> Self {
        MongoDBConnection { client, database }
    }
}

impl Connection for MongoDBConnection {
    async fn connect(url: Cow<'static, str>) -> Result<MongoDBConnection> {
        let context = format!("While trying to connect to `{}`", truncate_long!(url));
        let url = Self::sanitize_url(url)?;
        let client = Client::with_uri_str(&url).await.with_context(|| context)?;
        let database = client.database(
            url.path_segments()
                .and_then(|mut v| v.next())
                .unwrap_or_default(),
        );
        Ok(MongoDBConnection::new(client, database))
    }

    #[allow(refining_impl_trait)]
    async fn begin(&mut self) -> Result<MongoDBTransaction<'_>> {
        Err(Error::msg(
            "Transactions are not supported by this MongoDB driver",
        ))
    }
}

impl Executor for MongoDBConnection {
    type Driver;

    fn prepare(
        &mut self,
        query: String,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        todo!()
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        todo!()
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<RowLabeled>> + Send {
        self.run(query).filter_map(|v| async move {
            match v {
                Ok(QueryResult::Row(v)) => Some(Ok(v)),
                Err(e) => Some(Err(e)),
                _ => None,
            }
        })
    }

    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        self.run(query)
            .filter_map(|v| async move {
                match v {
                    Ok(QueryResult::Affected(v)) => Some(Ok(v)),
                    Err(e) => Some(Err(e)),
                    _ => None,
                }
            })
            .try_collect()
    }

    fn append<'a, E, It>(
        &mut self,
        entities: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        E: tank_core::Entity + 'a,
        It: IntoIterator<Item = &'a E> + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        let mut query = String::new();
        self.driver()
            .sql_writer()
            .write_insert(&mut query, entities, false);
        self.execute(query)
    }
}
