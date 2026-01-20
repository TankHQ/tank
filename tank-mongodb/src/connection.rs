use crate::{MongoDBDriver, MongoDBTransaction, Payload, RowWrap};
use async_stream::try_stream;
use mongodb::{Client, Database, bson::Bson};
use std::{borrow::Cow, future};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, QueryType, Result,
    RowsAffected,
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
        _query: String,
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
        let count = metadata.count;
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
            let payload = &prepared.payload;
            match query_type {
                QueryType::Select => {
                    if count == Some(1) {
                        let Payload::FindOne(payload) = &payload else {
                            Err(Error::msg(format!(
                                "Query is a select with count 1 but the payload {payload:?} is not the expected FindOne variant"
                            )))?;
                            return;
                        };
                        let options = &payload.options;
                        match collection
                            .find_one(payload.find.clone())
                            .with_options(options.clone())
                            .await
                        {
                            Ok(Some(v)) => {
                                yield QueryResult::Row(match v.0 {
                                    Cow::Borrowed(v) => v.clone(),
                                    Cow::Owned(v) => v,
                                })
                            }
                            Ok(None) => {}
                            Err(e) => Err(Error::msg(format!("{e}")))?,
                        }
                    } else {
                        let Payload::Find(payload) = &payload else {
                            Err(Error::msg(format!(
                                "Query is a select but the payload {payload:?} is not the expected FindOne variant"
                            )))?;
                            return;
                        };
                        let options = &payload.options;
                        let mut stream = collection
                            .find(payload.matching.clone())
                            .with_options(options.clone())
                            .await?;
                        while let Some(result) = stream.try_next().await? {
                            yield QueryResult::Row(match result.0 {
                                Cow::Borrowed(v) => v.clone(),
                                Cow::Owned(v) => v,
                            });
                        }
                    }
                }
                QueryType::InsertInto => {
                    if count == Some(1) {
                        let Payload::InsertOne(payload) = &payload else {
                            Err(Error::msg(format!(
                                "Query is a insert with count 1 but the payload {payload:?} is not the expected InsertOne variant"
                            )))?;
                            return;
                        };
                        let result = collection
                            .insert_one(RowWrap(Cow::Borrowed(&payload.row)))
                            .with_options(payload.options.clone())
                            .await?;
                        let last_affected_id = match result.inserted_id {
                            Bson::Int32(v) => Some(v as i64),
                            Bson::Int64(v) => Some(v),
                            _ => None,
                        };
                        yield QueryResult::Affected(RowsAffected {
                            rows_affected: Some(1),
                            last_affected_id,
                        });
                    } else {
                        let Payload::InsertMany(payload) = &payload else {
                            Err(Error::msg(format!(
                                "Query is a insert but the payload {payload:?} is not the expected InsertMany variant"
                            )))?;
                            return;
                        };
                        let len = payload.rows.len();
                        collection
                            .insert_many(payload.rows.iter().map(|v| RowWrap(Cow::Borrowed(v))))
                            .with_options(payload.options.clone())
                            .await?;
                        yield QueryResult::Affected(RowsAffected {
                            rows_affected: Some(len as _),
                            last_affected_id: None,
                        });
                    }
                }
                QueryType::DeleteFrom => {
                    let Payload::Delete(payload) = &payload else {
                        Err(Error::msg(format!(
                            "Query is a delete but the payload {payload:?} is not the expected Delete variant"
                        )))?;
                        return;
                    };
                    let result = if count == Some(1) {
                        collection
                            .delete_one(payload.matching.clone())
                            .with_options(payload.options.clone())
                            .await?
                    } else {
                        collection
                            .delete_many(payload.matching.clone())
                            .with_options(payload.options.clone())
                            .await?
                    };
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(result.deleted_count),
                        last_affected_id: None,
                    });
                }
                // There is no need for the following queries in MongoDB
                QueryType::CreateTable => {}
                QueryType::DropTable => {}
                QueryType::CreateSchema => {}
                QueryType::DropSchema => {},
            }
        }
    }
}
