use crate::{
    BatchPayload, CreateCollectionPayload, DeletePayload, DropCollectionPayload,
    DropDatabasePayload, FindManyPayload, FindOnePayload, InsertManyPayload, InsertOnePayload,
    MongoDBDriver, MongoDBTransaction, Payload, RowWrap, UpsertPayload,
};
use async_stream::try_stream;
use mongodb::{Client, Collection, Database, bson::Bson};
use std::{borrow::Cow, future, i64};
use tank_core::{
    AsQuery, Connection, Error, ErrorContext, Executor, Query, QueryResult, Result, RowsAffected,
    TableRef,
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
    pub fn database(&self, table: &TableRef) -> Database {
        let schema = &table.schema;
        if !schema.is_empty() {
            self.client.database(&schema)
        } else {
            self.default_database.clone()
        }
    }
    pub(crate) fn collection(&self, table: &TableRef) -> Collection<RowWrap<'_>> {
        if table.name.is_empty() {
            log::error!("Tried to get a collection from a empty table");
        }
        self.database(table).collection(&table.name)
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
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
                Err(Error::msg(
                    "Query is not the expected tank::Query::Prepared variant (MongoDB driver uses prepared)",
                ))?;
                return;
            };
            let payload = &prepared.get_payload();
            match payload {
                Payload::Fragment(..) => {
                    Err(Error::msg(format!(
                        "Cannot run a query with fragment variant {payload:?}"
                    )))?;
                    return;
                }
                Payload::FindOne(FindOnePayload {
                    table,
                    filter: Bson::Document(filter),
                    options,
                    ..
                }) => {
                    let collection = self.collection(table);
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
                }
                Payload::FindMany(FindManyPayload {
                    table,
                    filter: Bson::Document(filter),
                    options,
                    ..
                }) => {
                    let collection = self.collection(table);
                    let mut stream = collection
                        .find(filter.clone())
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                    while let Some(result) = stream
                        .try_next()
                        .await
                        .with_context(|| make_context!(payload))?
                    {
                        yield QueryResult::Row(match result.0 {
                            Cow::Borrowed(v) => v.clone(),
                            Cow::Owned(v) => v,
                        });
                    }
                }
                Payload::InsertOne(InsertOnePayload {
                    table,
                    row,
                    options,
                    ..
                }) => {
                    let collection = self.collection(table);
                    let result = collection
                        .insert_one(RowWrap(Cow::Borrowed(row)))
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                    let last_affected_id = match result.inserted_id {
                        Bson::Int32(v) => Some(v as i64),
                        Bson::Int64(v) => Some(v),
                        _ => None,
                    };
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(1),
                        last_affected_id,
                    });
                }
                Payload::InsertMany(InsertManyPayload {
                    table,
                    rows,
                    options,
                    ..
                }) => {
                    let collection = self.collection(table);
                    collection
                        .insert_many(rows.iter().map(|v| RowWrap(Cow::Borrowed(v))))
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                    yield QueryResult::Affected(RowsAffected {
                        rows_affected: Some(rows.len() as _),
                        last_affected_id: None,
                    });
                }
                Payload::Upsert(UpsertPayload {
                    table,
                    filter: Bson::Document(filter),
                    modifications,
                    options,
                    ..
                }) => {
                    let collection = self.collection(table);
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
                Payload::Delete(DeletePayload {
                    table,
                    filter: Bson::Document(filter),
                    options,
                    single,
                    ..
                }) => {
                    let collection = self.collection(table);
                    let result = if *single {
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
                Payload::Batch(BatchPayload { batch, options }) => {
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
                Payload::CreateCollection(CreateCollectionPayload { table, options, .. }) => {
                    let database = self.database(table);
                    database
                        .create_collection(table.name.to_string())
                        .with_options(options.clone())
                        .await
                        .with_context(|| make_context!(payload))?;
                }
                Payload::DropCollection(DropCollectionPayload { table }) => {
                    let collection = self.collection(table);
                    collection
                        .drop()
                        .await
                        .with_context(|| make_context!(payload))?;
                }
                Payload::CreateDatabase(..) => {
                    // No database creating needed (it is created automatically)
                }
                Payload::DropDatabase(DropDatabasePayload { table }) => {
                    let database = self.database(table);
                    database
                        .drop()
                        .await
                        .with_context(|| make_context!(payload))?;
                }
                _ => {
                    Err(Error::msg(format!(
                        "Unexpected payload in the query {payload:?}"
                    )))?;
                    return;
                }
            }
        }
        .map_err(move |e: Error| {
            log::error!("{e:#}");
            e
        })
    }
}
