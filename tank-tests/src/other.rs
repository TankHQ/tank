use std::{pin::pin, sync::LazyLock};
use tank::{
    DataSet, Entity, Executor, Value, cols,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Entity, PartialEq)]
pub struct ATable {
    #[tank(primary_key)]
    a_column: String,
}

pub async fn other<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    ATable::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ATable table");
    ATable::create_table(executor, true, true)
        .await
        .expect("Failed to create ATable table");
    ATable::insert_one(
        executor,
        &ATable {
            a_column: "".into(),
        },
    )
    .await
    .expect("Could not save a row");

    // SELECT NULL
    let value = pin!(
        ATable::table()
            .select(executor, cols!(NULL), true, None)
            .map_ok(|v| v.values.into_iter().nth(0).unwrap())
    )
    .next()
    .await
    .expect("No result returned from the stream")
    .expect("Could not query for NULL");
    assert!(value.is_null());
}
