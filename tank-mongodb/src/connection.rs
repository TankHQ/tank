use crate::{
    BatchPayload, DeletePayload, FindManyPayload, FindOnePayload, MongoDBDriver,
    MongoDBTransaction, Payload, RowWrap, UpsertPayload,
};
use async_stream::try_stream;
use mongodb::{Client, Database, bson::Bson};
use std::{borrow::Cow, future, i64};
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
        let client = Client::with_uri_str(&url)
            .await
            .with_context(|| context.clone())?;
        let database = client.database(match url.path_segments().and_then(|mut v| v.next()) {
            Some(v) if !v.is_empty() => v,
            _ => {
                let error = Error::msg("Empty database name").context(context);
                log::error!("{:#}", error);
                return Err(error);
            }
        });
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
        macro_rules! make_context {
            ($query:expr) => {
                format!(
                    "While running the query:\n{}",
                    truncate_long!(format!("{:?}", $query), true)
                )
            };
        }
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
            let payload = &prepared.get_payload();
            match query_type {
                QueryType::Select => {
                    if count == Some(1) {
                        let Payload::FindOne(FindOnePayload {
                            filter: Bson::Document(filter),
                            options,
                            ..
                        }) = &payload
                        else {
                            Err(Error::msg(format!(
                                "Query is a select with count 1 but the payload {payload:?} is not a FindOne with a Bson::Document matcher"
                            )))?;
                            return;
                        };
                        match collection
                            .find_one(filter.clone())
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
                            Err(e) => {
                                Err(Error::msg(format!("{e}"))).context(make_context!(payload))?;
                                return;
                            }
                        }
                    } else {
                        let Payload::FindMany(FindManyPayload {
                            filter: Bson::Document(filter),
                            options,
                            ..
                        }) = &payload
                        else {
                            Err(Error::msg(format!(
                                "Query is a select with but the payload {payload:?} is not a Payload::Find with a Bson::Document matcher"
                            )))?;
                            return;
                        };
                        let mut stream = collection
                            .find(filter.clone())
                            .with_options(options.clone())
                            .await
                            .with_context(|| make_context!(payload))?;
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
                                "Query is a insert with count 1 but the payload {payload:?} is not the expected Payload::InsertOne variant"
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
                                "Query is a insert but the payload {payload:?} is not the expected Payload::InsertMany variant"
                            )))?;
                            return;
                        };
                        let len = payload.rows.len();
                        collection
                            .insert_many(payload.rows.iter().map(|v| RowWrap(Cow::Borrowed(v))))
                            .with_options(payload.options.clone())
                            .await
                            .with_context(|| make_context!(payload))?;
                        yield QueryResult::Affected(RowsAffected {
                            rows_affected: Some(len as _),
                            last_affected_id: None,
                        });
                    }
                }
                QueryType::Upsert => {
                    let Payload::Upsert(UpsertPayload {
                        filter: Bson::Document(filter),
                        modifications,
                        options,
                        ..
                    }) = &payload
                    else {
                        Err(Error::msg(format!(
                            "Query is a upsert with count 1 but the payload {payload:?} is not the expected Payload::UpsertOne with a Bson::Document matcher"
                        )))?;
                        return;
                    };
                    let result = collection
                        .update_one(filter.clone(), modifications.clone())
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                    let last_affected_id = match result.upserted_id {
                        Some(Bson::Int32(v)) => Some(v as i64),
                        Some(Bson::Int64(v)) => Some(v),
                        _ => None,
                    };
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(result.modified_count),
                        last_affected_id,
                    });
                }
                QueryType::DeleteFrom => {
                    let Payload::Delete(DeletePayload {
                        filter: Bson::Document(filter),
                        options,
                        ..
                    }) = &payload
                    else {
                        Err(Error::msg(format!(
                            "Query is a delete but the payload {payload:?} is not the expected Payload::Delete with a Bson::Document matcher"
                        )))?;
                        return;
                    };
                    let result = if count == Some(1) {
                        collection.delete_one(filter.clone())
                    } else {
                        collection.delete_many(filter.clone())
                    }
                    .with_options(options.clone())
                    .await
                    .with_context(|| make_context!(payload))?;
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(result.deleted_count),
                        last_affected_id: None,
                    });
                }
                // There is no need for the following queries in MongoDB
                QueryType::CreateTable => {}
                QueryType::DropTable => {
                    collection.drop().await.with_context(|| make_context!(payload))?;
                }
                QueryType::CreateSchema => {}
                QueryType::DropSchema => {
                    database.drop().await.with_context(|| make_context!(payload))?;
                }
                QueryType::Batch => {
                    let Payload::Batch(BatchPayload { batch, options }) = &payload else {
                        Err(Error::msg(format!(
                            "Query is a batch with but the payload {} is not the expected Payload::Batch",
                            truncate_long!(format!("{payload:?}"), true),
                        )))?;
                        return;
                    };
                    let result = self
                        .client
                        .bulk_write(batch.iter().map(|v| v.as_write_models()).flatten())
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(
                            (result.inserted_count
                                + result.matched_count
                                + result.modified_count
                                + result.upserted_count
                                + result.deleted_count)
                                .clamp(0, i64::MAX as _) as _,
                        ),
                        last_affected_id: None,
                    })
                }
            }
        }
        .map_err(move |e: Error| {
            log::error!("{e:#}");
            e
        })
    }
}
