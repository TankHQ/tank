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
                        Err(Error::msg("Valkey: Query does not specify full Primary Key. Only exact PK lookup is supported."))?;
                        return;
                    }

                    let key = &payload.key_prefix;
                    let mut pipe = redis::pipe();
                    
                    // 1. Fetch Scalars
                    pipe.hgetall(key);                    
                    
                    // 2. Fetch Vectors
                    // We identify vector columns by checking the table definition
                    let mut vector_cols = Vec::new();
                    
                    if !payload.columns.is_empty() {
                         for col_name in &payload.columns {
                             if let Some(col_def) = payload.table.columns.iter().find(|c| c.name() == col_name) {
                                 if matches!(col_def.value, Value::List(_, _) | Value::Array(_, _, _) | Value::Map(_, _, _)) {
                                     vector_cols.push(col_name.clone());
                                 }
                             }
                         }
                    }

                    for col_name in &vector_cols {
                        let subkey = format!("{}:{}", key, col_name);
                        pipe.lrange(subkey, 0, -1);
                    }
                    
                    let results: Vec<redis::Value> = pipe.query_async(&mut self.connection).await.map_err(|e| Error::msg(e.to_string()))?;
                    
                    let mut row_values = Vec::new();
                    
                    // Parse HGETALL
                    if let Some(scalar_res) = results.first() {
                        if let redis::Value::Bulk(items) = scalar_res {
                            for chunks in items.chunks(2) {
                                if let [k_raw, v_raw] = chunks {
                                    let key_str = match k_raw {
                                        redis::Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                                        _ => continue,
                                    };
                                    
                                    let val = match v_raw {
                                        redis::Value::Data(bytes) => {
                                            let s = String::from_utf8_lossy(bytes).to_string();
                                            Value::Varchar(Some(s.into()))
                                        },
                                        redis::Value::Int(n) => Value::Int64(Some(*n)),
                                        redis::Value::Nil => Value::Null,
                                        _ => Value::Null 
                                    };
                                    row_values.push((key_str, val));
                                }
                            }
                        }
                    }
                    
                    // Parse Vectors
                    let mut result_idx = 1;
                    for col_name in &vector_cols {
                        if let Some(vec_res) = results.get(result_idx) {
                             if let redis::Value::Bulk(items) = vec_res {
                                 let list_vals: Vec<Value> = items.iter().map(|item| {
                                     match item {
                                         redis::Value::Data(bytes) => Value::Varchar(Some(String::from_utf8_lossy(bytes).to_string().into())),
                                         redis::Value::Int(n) => Value::Int64(Some(*n)),
                                         _ => Value::Null,
                                     }
                                 }).collect();
                                 row_values.push((col_name.to_string(), Value::List(Some(list_vals), Box::new(Value::Varchar(None)))));
                             }
                        }
                        result_idx += 1;
                    }
                    
                    if !row_values.is_empty() {
                         yield QueryResult::Row(RowLabeled(row_values));
                    }
                }
                Payload::Empty => {}
            }
        }
    }
}
