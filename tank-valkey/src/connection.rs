use crate::{ValkeyDriver, prepared::{ValkeyPrepared, Payload}};
use async_stream::try_stream;
use redis::{Client, aio::MultiplexedConnection, AsyncCommands, Pipeline};
use std::{borrow::Cow, sync::Arc};
use tank_core::{
    AsQuery, Connection, Error, Executor, Query, QueryResult, Result, RowLabeled,
    Value,
    stream::Stream,
};

pub struct ValkeyConnection {
    pub(crate) connection: MultiplexedConnection,
}

impl Connection for ValkeyConnection {
    async fn connect(url: Cow<'static, str>) -> Result<Self>
    where
        Self: Sized,
    {
        let context = Arc::new(format!("While trying to connect to `{}`", url));
        let client = Client::open(&*url).map_err(|e| Error::msg(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::msg(e.to_string()))?;
        Ok(Self { connection })
    }

    fn begin(
        &mut self,
    ) -> impl std::future::Future<Output = tank_core::Result<<Self::Driver as tank_core::Driver>::Transaction<'_>>>
    {
        async { todo!("Transaction support") }
    }
}

impl Executor for ValkeyConnection {
    type Driver = ValkeyDriver;

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        let mut query = query.as_query();
        try_stream! {
            let Query::Prepared(prepared) = query.as_mut() else {
               Err(Error::msg("Query is not prepared"))?;
               return;
            };

            let prepared = prepared
                .as_any()
                .downcast_mut::<ValkeyPrepared>()
                .ok_or_else(|| Error::msg("Prepared query is not ValkeyPrepared"))?;

            match &prepared.payload {
                Payload::Command(cmd) => {
                    let _ : () = cmd.query_async(&mut self.connection).await.map_err(|e| Error::msg(e.to_string()))?;
                    yield QueryResult::RowsAffected(0);
                }
                Payload::Select(payload) => {
                    if !payload.exact_key {
                        // Strict requirement: One roundtrip. Only PK lookup supported.
                        // If we fall here, SqlWriter failed to extract full PK.
                        // We yield nothing or error. Choosing to log and yield nothing.
                        // Actually, maybe yield error to inform user explicitly.
                        Err(Error::msg("Valkey: Query does not specify full Primary Key. Only exact PK lookup is supported."))?;
                        return;
                    }

                    let key = &payload.key_prefix;
                    let mut pipe = redis::pipe();

                    // 1. Fetch Scalars
                    // We use HGETALL to fetch all scalar fields. This allows us to discover fields
                    // that might not be in our strict TableRef definition (if the DB schema has drifted)
                    // and handles "SELECT *" naturally.
                    pipe.hgetall(key);

                    // 2. Fetch Vectors
                    let vector_cols: Vec<_> = payload.columns.iter().filter(|c| c.is_vector).collect();
                    for col in &vector_cols {
                        let subkey = format!("{}:{}", key, col.name);
                        pipe.lrange(subkey, 0, -1);
                    }

                    let results: Vec<redis::Value> = pipe.query_async(&mut self.connection).await.map_err(|e| Error::msg(e.to_string()))?;

                    // Parse results
                    // results[0] is always HGETALL result (Map/Array of pairs)
                    // results[1..] are vector results

                    let mut row_values = Vec::new();

                    // Parse HGETALL (Scalar fields)
                    if let Some(scalar_res) = results.first() {
                        // HGETALL returns Bulk(Array) of [Key, Value, Key, Value...]
                        if let redis::Value::Bulk(items) = scalar_res {
                            // Iterate in pairs
                            for chunks in items.chunks(2) {
                                if let [k_raw, v_raw] = chunks {
                                    let key_str = match k_raw {
                                        redis::Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                                        _ => continue,
                                    };

                                    // If we are projecting specific columns, we could filter here,
                                    // but retrieving everything is safer for * and discovery.

                                    let val = match v_raw {
                                        redis::Value::Data(bytes) => {
                                            let s = String::from_utf8_lossy(bytes).to_string();
                                            // Attempt simplistic type inference or just return string?
                                            // Tank usually wants specific types.
                                            // Without looking up the column definition, String is safest.
                                            Value::Varchar(Some(s.into()))
                                        },
                                        redis::Value::Int(n) => Value::Int64(Some(*n)),
                                        redis::Value::Nil => Value::Null,
                                        _ => Value::Null /* Ignore complex nested in HGETALL? */
                                    };

                                    row_values.push((key_str, val));
                                }
                            }
                        }
                    }

                    for col in &vector_cols {
                        if let Some(vec_res) = results.get(result_idx) {
                             // LRANGE returns Array
                             if let redis::Value::Bulk(items) = vec_res {
                                 // Convert items to Vec<Value>
                                 let list_vals: Vec<Value> = items.iter().map(|item| {
                                     match item {
                                         redis::Value::Data(bytes) => Value::Varchar(Some(String::from_utf8_lossy(bytes).to_string().into())),
                                         redis::Value::Int(n) => Value::Int64(Some(*n)),
                                         _ => Value::Null,
                                     }
                                 }).collect();

                                 // We need to wrap in Value::List or Array
                                 // Inner type? defaulting to Varchar for now
                                 row_values.push((col.name.clone(), Value::List(Some(list_vals), Box::new(Value::Varchar(None)))));
                             }
                        }
                        result_idx += 1;
                    }

                    // Only yield row if we found something (e.g. at least one non-null scalar or non-empty vector?)
                    // Or if key exists?
                    // With HGET/HMGET, it returns Nils if key missing.
                    // We might need to check if ALL scalars are Nil?
                    // User said "one roundtrip".
                    // If HMGET returns all Nils and vectors empty, row probably doesn't exist.

                    let has_data = row_values.iter().any(|(_, v)| !matches!(v, Value::Null));
                    if has_data {
                         yield QueryResult::Row(RowLabeled(row_values));
                    }
                }
                Payload::Empty => {}
            }
        }
    }
}
