use std::sync::Mutex;
use tank_chdb::{ChDBConnection, ChDBDriver};
use tank_core::{Connection, Executor, QueryResult, Result, Row, Value};

static MUTEX: Mutex<()> = Mutex::new(());

#[tokio::test]
async fn empty_select_keeps_schema() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let mut results = connection
        .run("SELECT number AS n FROM numbers(0)")
        .await
        .unwrap();
    let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
        panic!("schema-bearing response must return rows");
    };
    assert!(rows.next().is_none());
    assert!(results.next().is_none());
}

#[tokio::test]
async fn no_output_is_affected_unknown() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    {
        let mut results = connection
            .run("CREATE TABLE no_output (n UInt8) ENGINE=Memory")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Affected(affected))) = results.next() else {
            panic!("no output must return effects");
        };
        assert_eq!(affected.rows_affected, None);
        assert!(results.next().is_none());
    }
    connection.execute("DROP TABLE no_output").await.unwrap();
}

#[tokio::test]
async fn metadata_orders_columns_and_preserves_json_conversion() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let row = connection
        .fetch("SELECT 'hello' AS `t.z`, toUInt8(7) AS a, CAST(NULL AS Nullable(String)) AS nil")
        .await
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.names(), ["z", "a", "nil"]);
    assert_eq!(
        row.value(0).unwrap().as_ref(),
        &Value::Varchar(Some("hello".into()))
    );
    assert_eq!(
        row.value(1).unwrap().into_owned(),
        Value::Json(Some(serde_json::json!(7)))
    );
    assert_eq!(row.value(2).unwrap().as_ref(), &Value::Null);
}

#[tokio::test]
async fn malformed_output_is_not_misclassified_as_affected() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    assert!(connection.execute("SELECT (").await.is_err());
    assert!(
        connection
            .execute("SELECT * FROM no_such_value_wrap_table")
            .await
            .is_err()
    );
    assert_eq!(
        connection
            .fetch("SELECT 1")
            .await
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn compact_metadata_preserves_duplicate_labels_and_positions() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let row = connection
        .fetch("SELECT * FROM (SELECT 1 AS n) a, (SELECT 2 AS n) b")
        .await
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.names(), ["n", "n"]);
    assert_eq!(
        row.value(0).unwrap().into_owned(),
        Value::Json(Some(1.into()))
    );
    assert_eq!(
        row.value(1).unwrap().into_owned(),
        Value::Json(Some(2.into()))
    );
}
