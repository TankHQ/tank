// use std::{borrow::Cow, collections::HashSet, sync::LazyLock};
// use tank::{Entity, Executor, Result, expr, stream::TryStreamExt};
// use tokio::sync::Mutex;

// static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// #[derive(Clone, Debug, PartialEq, Eq)]
// pub enum Method {
//     GET,
//     POST,
//     PUT,
//     DELETE,
// }

// #[derive(Entity)]
// #[tank(schema = "api")]
// pub struct Request {
//     #[tank(primary_key)]
//     pub id: i64,
//     pub target: String,
//     pub method: Method,
//     pub beign_timestamp_ms: i64,
//     pub end_timestamp_ms: Option<i64>,
// }

// pub async fn requests<E: Executor>(connection: &mut E) -> Result<()> {
//     let _lock = MUTEX.lock();
// }
