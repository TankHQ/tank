// use std::{borrow::Cow, collections::HashSet, sync::LazyLock};
// use tank::{
//     AsValue, Entity, Error, Executor, QueryBuilder, Result, Value, cols, expr, join,
//     stream::TryStreamExt,
// };
// use tokio::sync::Mutex;

// static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// #[derive(Clone, Debug, PartialEq, Eq)]
// pub enum Method {
//     GET,
//     POST,
//     PUT,
//     DELETE,
// }
// impl AsValue for Method {
//     fn as_empty_value() -> Value {
//         Value::Varchar(None)
//     }
//     fn as_value(self) -> Value {
//         Value::Varchar(Some(
//             match self {
//                 Method::GET => "get",
//                 Method::POST => "post",
//                 Method::PUT => "put",
//                 Method::DELETE => "delete",
//             }
//             .into(),
//         ))
//     }
//     fn try_from_value(value: Value) -> Result<Self>
//     where
//         Self: Sized,
//     {
//         if let Value::Varchar(Some(v)) = value.try_as(&String::as_empty_value())? {
//             match &*v {
//                 "get" => return Ok(Method::GET),
//                 "post" => return Ok(Method::POST),
//                 "put" => return Ok(Method::PUT),
//                 "delete" => return Ok(Method::DELETE),
//                 _ => {
//                     return Err(Error::msg(format!(
//                         "Unexpected value `{v}` for Method enum"
//                     )));
//                 }
//             }
//         }
//         Err(Error::msg("Unexpected value for Method enum"))
//     }
// }

// #[derive(Default, Entity, PartialEq, Eq)]
// struct RequestLimit {
//     #[tank(primary_key)]
//     pub target_pattern: &'static str,
//     pub requests: i32,
//     pub method: Option<Method>,
//     // If set, it means maximum request in unit of time, otherwise means maximum concurrent requests
//     pub interval_ms: Option<i32>,
// }

// #[derive(Entity, PartialEq, Eq)]
// #[tank(schema = "api")]
// pub struct Request {
//     #[tank(primary_key)]
//     pub id: i64,
//     pub target: String,
//     pub method: Option<Method>,
//     pub beign_timestamp_ms: i64,
//     pub end_timestamp_ms: Option<i64>,
// }

// pub async fn requests<E: Executor>(executor: &mut E) {
//     let _lock = MUTEX.lock();

//     // Setup
//     RequestLimit::drop_table(executor, true, false)
//         .await
//         .expect("Could not drop the RequestLimit table");
//     Request::drop_table(executor, true, false)
//         .await
//         .expect("Could not drop the Request table");

//     RequestLimit::create_table(executor, false, true)
//         .await
//         .expect("Could not create the RequestLimit table");
//     Request::create_table(executor, false, false)
//         .await
//         .expect("Could not create the Request table");

//     // Request limits
//     RequestLimit::insert_many(
//         executor,
//         &[
//             // Max 10 concurrent requests
//             RequestLimit {
//                 target_pattern: "v1/%",
//                 requests: 10,
//                 ..Default::default()
//             },
//             // Max 5 data concurrent requests
//             RequestLimit {
//                 target_pattern: "v1/server/data/%",
//                 requests: 5,
//                 ..Default::default()
//             },
//             // Max 1 user concurrent delete request
//             RequestLimit {
//                 target_pattern: "v1/server/users/%",
//                 requests: 1,
//                 method: Method::DELETE.into(),
//                 ..Default::default()
//             },
//             // Max 100 requests
//             RequestLimit {
//                 target_pattern: "v2/%",
//                 requests: 100,
//                 interval_ms: 60_000.into(),
//                 ..Default::default()
//             },
//         ],
//     )
//     .await
//     .expect("Could not insert the limits");

//     let query = QueryBuilder::new()
//         .select(cols!(*))
//         .from(join!(RequestLimit CROSS Request))
//         .where_expr(expr!(
//             // Filter request limits that apply to this request target
//             ? == RequestLimit::target as LIKE
//             // Filter requests that apply to the request limit
//             && Request::target == RequestLimit::target as LIKE
//             && (
//                 RequestLimit::method == NULL
//                 || RequestLimit::method == Request::method
//             )
//             &&
//                 RequestLimit::interval_ms == NULL
//                 ||
//                 RequestLimit::interval_ms >
//                     self.db_client
//                         .timestamp_ms_expr()
//                         .sub(request_limit::Column::TimeIntervalMs.into_simple_expr()),
//                 ),
//             ]
//         ))
//         .group_by_col(request_limit::Column::Id.as_column_ref())
//         .cond_having(
//             request_limit::Column::Requests
//                 .into_expr()
//                 .lte(request::Column::Id.count()),
//         )
//         .to_string(DBClient::SQL_QUERY_BUILDER);
// }
