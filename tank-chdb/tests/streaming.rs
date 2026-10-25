use std::sync::Mutex;
use tank_chdb::{ChDBConnection, ChDBDriver};
use tank_core::{
    AsValue, Connection, Executor, QueryParam, QueryResult, RawQuery, Result, Row, Value,
};

static MUTEX: Mutex<()> = Mutex::new(());

#[tokio::test]
async fn streaming_public_execution_and_reuse() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();

    // Dropping an unconsumed statement iterator also owns/cancels its rowset.
    let pending = connection
        .run("SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1")
        .await
        .unwrap();
    drop(pending);
    // Cancel while the blocking initialization worker may still own a stream.
    // Reuse must wait for that worker's native cancellation and destruction.
    for _ in 0..4 {
        let mut future = Box::pin(connection.run(
            "SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1",
        ));
        std::future::poll_fn(|cx| {
            let _ = future.as_mut().poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        drop(future);
    }
    assert_eq!(
        connection
            .fetch("SELECT 123")
            .await
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .len(),
        1
    );
    let marker = "{p0:Nullable(String)}";
    let mut prepared = connection.prepare(RawQuery::with_params(
        format!("SELECT {marker} AS value FROM numbers(100) SETTINGS max_block_size=2, max_threads=1"),
        vec![QueryParam { begin: 7, end: 7 + marker.len() }],
    )).await.unwrap();
    for value in [7_i64, 11_i64] {
        prepared.bind(value).unwrap();
        let mut results = connection.run(&mut prepared).await.unwrap();
        let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
            panic!("prepared SET leaked as a statement")
        };
        let row = rows.next().unwrap().unwrap();
        assert_eq!(i64::try_from_value(row.value(0).unwrap()).unwrap(), value);
        assert!(results.next().is_none());
        // Drop early, rebind and reuse the same prepared statement.
    }
    {
        let mut rows = connection.fetch("SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1").await.unwrap();
        let first = rows.next().unwrap().unwrap().into_owned();
        for _ in 0..10 {
            rows.next().unwrap().unwrap();
        }
        assert_eq!(first.names(), ["number"]);
        assert_eq!(
            first.value(0).unwrap().as_ref(),
            &Value::Json(Some(0.into()))
        );
    }

    for sql in [
        "/* comment */ SELECT 1 WHERE 0",
        "WITH 1 AS n SELECT n WHERE 0",
    ] {
        let mut results = connection.run(sql).await.unwrap();
        let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
            panic!("empty SELECT must be a rowset")
        };
        assert!(rows.next().is_none());
        assert!(results.next().is_none());
    }

    let sql = "SELECT intDiv(42, toInt64(8-number)) FROM numbers(100) SETTINGS max_block_size=2, max_threads=1";
    {
        let mut rows = connection.fetch(sql).await.unwrap();
        assert!(
            rows.next().unwrap().is_ok(),
            "must yield rows before late failure"
        );
        let error = rows.collect::<Result<Vec<_>>>().unwrap_err();
        assert!(format!("{error:#}").contains("Division by zero"));
        assert!(format!("{error:#}").contains(sql));
    }

    let mut invalid = connection.run("SELECT (").await.unwrap();
    assert!(invalid.next().unwrap().is_err());
    assert!(invalid.next().is_none());
    drop(invalid);
    {
        let mut results = connection.run("SELECT 1 FORMAT Null").await.unwrap();
        assert!(
            results.next().unwrap().is_err(),
            "missing output schema must be an error, not empty success"
        );
    }

    // Unsupported ASTs are rejected by native initialization, then executed once.
    connection
        .execute("CREATE TABLE fallback_once (n UInt64) ENGINE=Memory")
        .await
        .unwrap();
    connection
        .execute("INSERT INTO fallback_once VALUES (1), (2)")
        .await
        .unwrap();
    let rows = connection
        .fetch("SELECT n FROM fallback_once")
        .await
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    drop(rows);
    let mut results = connection.run("SHOW TABLES").await.unwrap();
    let QueryResult::Rows(rows) = results.next().unwrap().unwrap() else {
        panic!("SHOW must retain buffered schema")
    };
    assert!(!rows.collect::<Result<Vec<_>>>().unwrap().is_empty());
    drop(results);
    connection
        .execute("DROP TABLE fallback_once")
        .await
        .unwrap();
    let mut results = connection
        .run("SHOW TABLES LIKE 'no_such_incremental_table'")
        .await
        .unwrap();
    let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
        panic!("empty SHOW must retain schema")
    };
    assert!(rows.next().is_none());
}

#[tokio::test]
async fn streaming_labels_filters_and_writes() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();

    // Qualified labels are reduced to their column name.
    {
        let row = connection
            .fetch("SELECT number AS `t.n`, 'hello' AS s, CAST(NULL AS Nullable(Int64)) AS nil FROM numbers(3) SETTINGS max_block_size=2, max_threads=1")
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.names(), ["n", "s", "nil"]);
        assert_eq!(
            row.value(1).unwrap().as_ref(),
            &Value::Varchar(Some("hello".into()))
        );
        assert_eq!(row.value(2).unwrap().as_ref(), &Value::Null);
    }

    // Filtered native streams yield the exact number of matching rows.
    for (sql, expected) in [
        (
            "SELECT number FROM numbers(100) WHERE number >= 90 SETTINGS max_block_size=2, max_threads=1",
            10,
        ),
        (
            "SELECT number FROM numbers(100) WHERE number % 3 = 0 SETTINGS max_block_size=2, max_threads=1",
            34,
        ),
    ] {
        let count = {
            let rows = connection
                .fetch(sql)
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            rows.len()
        };
        assert_eq!(count, expected);
    }

    // Statements without a native stream fall back and report unknown effects.
    for sql in [
        "CREATE TABLE streaming_fallback (n UInt64) ENGINE=Memory",
        "INSERT INTO streaming_fallback VALUES (7)",
        "INSERT INTO streaming_fallback SELECT number FROM numbers(3)",
        "SET max_threads=1",
    ] {
        let mut results = connection.run(sql).await.unwrap();
        let Some(Ok(QueryResult::Affected(affected))) = results.next() else {
            panic!("write returned rows: {sql}")
        };
        assert_eq!(
            affected.rows_affected, None,
            "storage reads must not become write counts"
        );
        assert!(results.next().is_none());
    }
    {
        let rows = connection
            .fetch("SELECT n FROM streaming_fallback")
            .await
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 4);
    }
    assert!(
        connection
            .execute("SELECT * FROM no_such_streaming_table")
            .await
            .is_err()
    );
    assert!(connection.execute("SELECT (").await.is_err());
    connection
        .execute("DROP TABLE streaming_fallback")
        .await
        .unwrap();
    {
        let rows = connection
            .fetch("SELECT 123")
            .await
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
    }
}
