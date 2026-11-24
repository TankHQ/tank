use std::{pin::pin, sync::Mutex};
use tank::{
    Connection, Executor, QueryResult, Row, Value,
    stream::{StreamExt, TryStreamExt},
};
use tank_chdb::{ChDBConnection, ChDBDriver};

static MUTEX: Mutex<()> = Mutex::new(());

#[tokio::test]
async fn empty_select_keeps_schema() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let mut results = pin!(connection.run("SELECT number AS n FROM numbers(0)"));
    assert!(results.next().await.is_none());
}

#[tokio::test]
async fn no_output_is_affected_unknown() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    {
        let mut results = pin!(connection.run("CREATE TABLE no_output (n UInt8) ENGINE=Memory"));
        let Some(Ok(QueryResult::Affected(affected))) = results.next().await else {
            panic!("No output must return effects");
        };
        assert_eq!(affected.rows_affected, None);
        assert!(results.next().await.is_none());
    }
    connection.execute("DROP TABLE no_output").await.unwrap();
}

#[tokio::test]
async fn metadata_orders_columns_and_preserves_json_conversion() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let mut rows =
        pin!(connection.fetch(
            "SELECT 'hello' AS `t.z`, toUInt8(7) AS a, CAST(NULL AS Nullable(String)) AS nil"
        ));
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.labels(), ["z", "a", "nil"]);
    assert_eq!(
        row.value(0).unwrap().as_ref(),
        &Value::Varchar(Some("hello".into()))
    );
    assert_eq!(row.value(1).unwrap().into_owned(), Value::UInt8(Some(7)));
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
            .try_collect::<Vec<_>>()
            .await
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
    let mut rows = pin!(connection.fetch("SELECT * FROM (SELECT 1 AS n) a, (SELECT 2 AS n) b"));
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.labels(), ["n", "n"]);
    assert_eq!(row.value(0).unwrap().into_owned(), Value::UInt8(Some(1)));
    assert_eq!(row.value(1).unwrap().into_owned(), Value::UInt8(Some(2)));
}

#[tokio::test]
async fn native_complex_types() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();
    let rows = connection
        .fetch(
            "SELECT \
                toLowCardinality('alpha') AS lc, \
                CAST(NULL AS LowCardinality(Nullable(String))) AS lc_null, \
                toFixedString('hi', 4) AS fs, \
                [toInt32(10), toInt32(20)] AS arr, \
                map('k', toInt32(7)) AS mp \
             FROM numbers(5) SETTINGS max_block_size=2, max_threads=1",
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // Verifies retained rows across multiple chunks stay intact.
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.value(0).unwrap().as_ref(), &Value::Varchar(Some("alpha".into())));
        assert_eq!(row.value(1).unwrap().as_ref(), &Value::Null);
        assert_eq!(row.value(2).unwrap().as_ref(), &Value::Varchar(Some("hi".into())));
        assert!(matches!(row.value(3).unwrap().as_ref(), Value::List(Some(v), _) if v.len() == 2));
        assert!(matches!(row.value(4).unwrap().as_ref(), Value::Map(Some(m), _) if m.len() == 1));
    }
}
