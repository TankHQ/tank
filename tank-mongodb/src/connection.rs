use crate::{MongoDBDriver, MongoDBPrepared, MongoDBTransaction};
use async_stream::try_stream;
use mongodb::{Client, Database, bson, options::ClientOptions};
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, RawQuery, Result,
    RowLabeled, RowsAffected, Value as TankValue,
    stream::{self, Stream},
    truncate_long,
};

/// Minimal MongoDB connection wrapper used by the driver.
pub struct MongoDBConnection {
    client: Client,
    default_database: Database,
}

impl MongoDBConnection {
    pub fn new(client: Client, default_database: Database) -> Self {
        MongoDBConnection {
            client,
            default_database,
        }
    }
    pub(crate) fn database(&self, query: &Query<MongoDBDriver>) -> Database {
        let schema = &query.table().schema;
        if !schema.is_empty() {
            self.client.database(&schema)
        } else {
            self.default_database.clone()
        }
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
    type Driver = MongoDBDriver;

    fn prepare(
        &mut self,
        query: RawQuery,
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
        let mut query = query.as_query();
        let query = query.as_mut();
        let database = self.database(query);
        let collection = database.collection(&query.table().name);
        if query.limit() == Some(1) {
            collection.find(filter)
        }
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
        let mut query = DynQuery::default();
        self.driver()
            .sql_writer()
            .write_insert(&mut query, entities, false);
        self.execute(query)
    }
}
