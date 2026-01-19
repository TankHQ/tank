use crate::{MongoDBDriver, MongoDBTransaction, Options, Payload, RowWrap};
use async_stream::try_stream;
use mongodb::{Client, Database};
use std::{borrow::Cow, future};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, QueryType, Result,
    stream::{Stream, TryStreamExt},
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
        let schema = &query.metadata().table.schema;
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
        query: String,
    ) -> impl Future<Output = Result<Query<Self::Driver>>> + Send {
        future::ready(Err(Error::msg("MongoDB does not support prepare")))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let database = self.database(query.as_mut());
        let metadata = query.as_mut().metadata();
        let query_type = metadata.query_type;
        let limit = metadata.limit;
        let collection = database.collection::<RowWrap>(&metadata.table.name);
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
                Err(Error::msg(
                    "Query is not the expected tank::Query::Prepared variant (MongoDB driver uses only the Prepared query object)",
                ))?;
                return;
            };
            let Some(query_type) = query_type else {
                Err(Error::msg("Query type is missing from the query metadata"))?;
                return;
            };
            match query_type {
                QueryType::Select => {
                    let Payload::Find(payload) = &prepared.payload else {
                        Err(Error::msg(
                            "Query is not the expected tank_mongodb::Payload::Find variant",
                        ))?;
                        return;
                    };
                    let options = &payload.options;
                    if limit == Some(1) {
                        let Options::FindOne(options) = options else {
                            Err(Error::msg(format!(
                                "Query payload options is {options:?} instead of the expected FindOne variant"
                            )))?;
                            return;
                        };
                        match collection
                            .find_one(payload.find.clone())
                            .with_options(options.clone())
                            .await
                        {
                            Ok(Some(v)) => yield QueryResult::Row(v.0),
                            Ok(None) => {}
                            Err(e) => Err(Error::msg(format!("{e}")))?,
                        }
                    } else {
                        let Options::Find(options) = &payload.options else {
                            Err(Error::msg(format!(
                                "Query payload options is {options:?} instead of the expected Find variant"
                            )))?;
                            return;
                        };
                        let mut stream = collection
                            .find(payload.find.clone())
                            .with_options(options.clone())
                            .await?;
                        while let Some(result) = stream.try_next().await? {
                            yield QueryResult::Row(result.0);
                        }
                    }
                }
                QueryType::InsertInto => {
                    let Payload::Insert(payload) = &prepared.payload else {
                        Err(Error::msg(
                            "Query is not the expected tank_mongodb::Payload::Insert variant",
                        ))?;
                        return;
                    };
                    let Options::Find(options) = &payload.options else {
                        Err(Error::msg(format!(
                            "Query has limit {limit:?}, but options is not tank_mongodb::Options::FindOne"
                        )))?;
                        return;
                    };
                    let docs = payload.documents;
                    collection.insert_many(docs).with_options(value)
                }
                QueryType::DeleteFrom => todo!(),
                QueryType::CreateTable => todo!(),
                QueryType::DropTable => todo!(),
                QueryType::CreateSchema => todo!(),
                QueryType::DropSchema => todo!(),
            }
        }
    }

    // fn execute<'s>(
    //     &'s mut self,
    //     query: impl AsQuery<'s, Self::Driver>,
    // ) -> impl Future<Output = Result<RowsAffected>> + Send {
    //     self.run(query)
    //         .filter_map(|v| async move {
    //             match v {
    //                 Ok(QueryResult::Affected(v)) => Some(Ok(v)),
    //                 Err(e) => Some(Err(e)),
    //                 _ => None,
    //             }
    //         })
    //         .try_collect()
    // }

    // fn append<'a, E, It>(
    //     &mut self,
    //     entities: It,
    // ) -> impl Future<Output = Result<RowsAffected>> + Send
    // where
    //     E: tank_core::Entity + 'a,
    //     It: IntoIterator<Item = &'a E> + Send,
    //     <It as IntoIterator>::IntoIter: Send,
    // {
    //     let mut query = DynQuery::default();
    //     self.driver()
    //         .sql_writer()
    //         .write_insert(&mut query, entities, false);
    //     self.execute(query)
    // }
}
