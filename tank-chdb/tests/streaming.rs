use std::{pin::pin, sync::Mutex};
use tank_chdb::{ChDBConnection, ChDBDriver};
use tank_core::{
    AsValue, Connection, Executor, QueryParam, QueryResult, RawQuery, Row, Value,
    stream::{Stream, StreamExt, TryStreamExt},
};

static MUTEX: Mutex<()> = Mutex::new(());

#[tokio::test]
async fn streaming_public_execution_and_reuse() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();

    // Dropping an unconsumed statement stream also owns/cancels its rowset.
    let pending = connection
        .run("SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1");
    drop(pending);
    // Cancel while the blocking initialization worker may still own a stream.
    // Reuse must wait for that worker's native cancellation and destruction.
    for _ in 0..4 {
        let mut stream = Box::pin(connection.run(
            "SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1",
        ));
        std::future::poll_fn(|cx| {
            let _ = stream.as_mut().poll_next(cx);
            std::task::Poll::Ready(())
        })
        .await;
        drop(stream);
    }
    assert_eq!(
        connection
            .fetch("SELECT 123")
            .try_collect::<Vec<_>>()
            .await
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
        let mut results = pin!(connection.run(&mut prepared));
        let QueryResult::Row(row) = results.next().await.unwrap().unwrap() else {
            panic!("Prepared SET leaked as a statement")
        };
        assert_eq!(i64::try_from_value(row.value(0).unwrap()).unwrap(), value);
        // Drop early, rebind and reuse the same prepared statement.
    }
    {
        let mut rows = pin!(connection.fetch(
            "SELECT number FROM numbers(1000000000000) SETTINGS max_block_size=2, max_threads=1"
        ));
        let first = rows.next().await.unwrap().unwrap().into_owned();
        for _ in 0..10 {
            rows.next().await.unwrap().unwrap();
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
        let mut results = pin!(connection.run(sql));
        assert!(results.next().await.is_none());
    }

    let sql = "SELECT intDiv(42, toInt64(8-number)) FROM numbers(100) SETTINGS max_block_size=2, max_threads=1";
    {
        let mut rows = pin!(connection.fetch(sql));
        assert!(
            rows.next().await.unwrap().is_ok(),
            "Must yield rows before late failure"
        );
        let error = rows.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(format!("{error:#}").contains("Division by zero"));
        assert!(format!("{error:#}").contains(sql));
    }

    {
        let mut invalid = pin!(connection.run("SELECT ("));
        assert!(invalid.next().await.unwrap().is_err());
        assert!(invalid.next().await.is_none());
    }
    {
        let mut results = pin!(connection.run("SELECT 1 FORMAT Null"));
        assert!(
            results.next().await.unwrap().is_err(),
            "Missing output schema must be an error, not empty success"
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
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    drop(rows);
    {
        let rows = connection
            .run("SHOW TABLES")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|r| matches!(r, QueryResult::Row(_))));
    }
    connection
        .execute("DROP TABLE fallback_once")
        .await
        .unwrap();
    {
        let mut results = pin!(connection.run("SHOW TABLES LIKE 'no_such_incremental_table'"));
        assert!(results.next().await.is_none());
    }
}

#[tokio::test]
async fn streaming_labels_filters_and_writes() {
    let _guard = MUTEX.lock().unwrap();
    let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
        .await
        .unwrap();

    // Qualified labels are reduced to their column name.
    {
        let mut rows = pin!(connection.fetch(
            "SELECT number AS `t.n`, 'hello' AS s, CAST(NULL AS Nullable(Int64)) AS nil FROM numbers(3) SETTINGS max_block_size=2, max_threads=1"
        ));
        let row = rows.next().await.unwrap().unwrap();
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
                .try_collect::<Vec<_>>()
                .await
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
        let mut results = pin!(connection.run(sql));
        let Some(Ok(QueryResult::Affected(affected))) = results.next().await else {
            panic!("Write returned rows: {sql}")
        };
        assert_eq!(
            affected.rows_affected, None,
            "Storage reads must not become write counts"
        );
        assert!(results.next().await.is_none());
    }
    {
        let rows = connection
            .fetch("SELECT n FROM streaming_fallback")
            .try_collect::<Vec<_>>()
            .await
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
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }
}
