use crate::{MongoDBDriver, MongoDBPrepared, MongoDBTransaction};
use futures::TryStreamExt;
use mongodb::{bson, options::ClientOptions, Client, Database};
use async_stream::try_stream;
use std::borrow::Cow;
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, Result, RowLabeled, RowsAffected,
    stream::{self, Stream}, Value as TankValue, truncate_long,
};

/// Minimal MongoDB connection wrapper used by the driver.
pub struct MongoDBConnection {
    client: Client,
    default_db: String,
}

impl MongoDBConnection {
    fn database(&self, name: Option<&str>) -> Database {
        let db = name
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.default_db.clone());
        self.client.database(&db)
    }
}

impl Executor for MongoDBConnection {
    type Driver = MongoDBDriver;

    async fn prepare(&mut self, _query: String) -> Result<Query<Self::Driver>> {
        Ok(Query::Prepared(MongoDBPrepared::new()))
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        let mut owned = std::mem::take(query.as_mut());
        let db = self.default_db.clone();
        let client = self.client.clone();
        try_stream! {
            let sql = owned.trim();
            let lower = sql.to_lowercase();

            // Special direct commands prefixed with `MONGO:` are supported.
            if lower.starts_with("mongo:") {
                let cmd = sql[6..].trim();
                if let Some(rest) = cmd.strip_prefix("create_collection") {
                    let name = rest.trim();
                    let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                    let database = client.database(db_name.unwrap_or(&db));
                    database.create_collection(coll, None).await?;
                    yield QueryResult::Affected(RowsAffected::default());
                    return;
                }
                if let Some(rest) = cmd.strip_prefix("drop_collection") {
                    let name = rest.trim();
                    let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                    let database = client.database(db_name.unwrap_or(&db));
                    database.collection::<bson::Document>(coll).drop(None).await?;
                    yield QueryResult::Affected(RowsAffected::default());
                    return;
                }
                if let Some(rest) = cmd.strip_prefix("insert") {
                    let rest = rest.trim();
                    if let Some((name, json)) = rest.split_once(' ') {
                        let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                        let database = client.database(db_name.unwrap_or(&db));
                        let coll_handle = database.collection::<bson::Document>(coll);
                        let doc: bson::Document = bson::from_document(bson::Document::from_reader(&mut json.as_bytes()).unwrap_or_default()).unwrap_or_default();
                        // Try inserting as a single document; for simplicity we ignore complex parsing errors.
                        coll_handle.insert_one(doc, None).await?;
                        yield QueryResult::Affected(RowsAffected { rows_affected: Some(1), last_affected_id: None });
                        return;
                    }
                }
            }

            // DDL: CREATE TABLE -> create collection
            if lower.starts_with("create table") {
                // crude parse for table name after CREATE TABLE [IF NOT EXISTS]
                let mut parts = sql.split_whitespace();
                // skip CREATE TABLE
                let _ = parts.next();
                let _ = parts.next();
                let mut name = parts.next().unwrap_or("");
                if name.eq_ignore_ascii_case("if") {
                    // skip IF NOT EXISTS
                    let _ = parts.next();
                    let _ = parts.next();
                    name = parts.next().unwrap_or("");
                }
                // strip potential parentheses
                name = name.trim_end_matches('(').trim_end_matches(')').trim_end_matches(';');
                let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                let database = client.database(db_name.unwrap_or(&db));
                database.create_collection(coll, None).await?;
                yield QueryResult::Affected(RowsAffected::default());
                return;
            }

            if lower.starts_with("drop table") {
                let mut parts = sql.split_whitespace();
                let _ = parts.next();
                let _ = parts.next();
                let name = parts.next().unwrap_or("").trim_end_matches(';');
                let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                let database = client.database(db_name.unwrap_or(&db));
                database.collection::<bson::Document>(coll).drop(None).await?;
                yield QueryResult::Affected(RowsAffected::default());
                return;
            }

            // Query: SELECT * FROM <table>
            if lower.starts_with("select") {
                // very small parser accepting only `SELECT * FROM name` without WHERE
                let from_pos = lower.find(" from ");
                if let Some(pos) = from_pos {
                    let after = &sql[pos + 6..];
                    let name = after.split_whitespace().next().unwrap_or("").trim_end_matches(';');
                    let (db_name, coll) = if let Some((a,b)) = name.split_once('.') { (Some(a), b) } else { (None, name) };
                    let database = client.database(db_name.unwrap_or(&db));
                    let coll_handle = database.collection::<bson::Document>(coll);
                    let mut cursor = coll_handle.find(None, None).await?;
                    while let Some(doc) = cursor.try_next().await? {
                        // Convert bson::Document into serde_json::Value, then into Tank::Value::Json
                        let json = bson::to_bson(&doc).ok().and_then(|b| bson::Bson::into_relaxed_extjson(&b).ok()).and_then(|s| serde_json::from_str(&s).ok()).unwrap_or(serde_json::Value::Null);
                        let names = doc.keys().cloned().collect::<Vec<_>>();
                        let names = std::sync::Arc::from(names);
                        let values = doc
                            .into_iter()
                            .map(|(_k, v)| TankValue::Json(Some(bson::from_bson::<serde_json::Value>(v).unwrap_or(serde_json::Value::Null))))
                            .collect::<Vec<_>>().into_boxed_slice();
                        yield QueryResult::Row(RowLabeled::new(names, values));
                    }
                    return;
                }
            }

            // Fallback: return an error that the SQL is unsupported.
            Err(Error::msg(format!("Unsupported SQL for MongoDB driver: {}", truncate_long!(sql))))?;
        }
    }
}

impl Connection for MongoDBConnection {
    async fn connect(url: Cow<'static, str>) -> Result<MongoDBConnection> {
        let context = format!("While trying to connect to `{}`", truncate_long!(url));
        // Parse client options and create a client. The mongodb URI may include a default database.
        let url_ref = url.as_ref();
        let mut options = ClientOptions::parse(url_ref).await.map_err(|e| Error::new(e).context(context.clone()))?;
        let default_db = options
            .default_database
            .clone()
            .unwrap_or_else(|| "test".to_string());
        let client = Client::with_options(options).map_err(|e| Error::new(e).context(context.clone()))?;
        Ok(MongoDBConnection { client, default_db })
    }

    #[allow(refining_impl_trait)]
    async fn begin(&mut self) -> Result<MongoDBTransaction<'_>> {
        Err(Error::msg("Transactions are not supported by this MongoDB driver"))
    }
}
