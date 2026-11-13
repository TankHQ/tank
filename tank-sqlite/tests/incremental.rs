use std::pin::pin;
use tank::{
    AsValue, Connection, DynQuery, Executor, QueryResult, Row, Sql, Value, cols,
    stream::{StreamExt, TryStreamExt},
};
use tank_sqlite::{SQLiteConnection, SQLiteDriver};
use tank_tests::init_logs;

#[tokio::test]
async fn late_step_errors_reusable() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let sql = "SELECT 1 UNION ALL SELECT abs(-9223372036854775808)";
    {
        let mut rows = pin!(connection.fetch(sql));
        let first = rows.next().await.unwrap().unwrap();
        assert_eq!(first.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
        let error = rows.next().await.unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("integer overflow"));
        assert!(rows.next().await.is_none());
        assert_eq!(first.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
    }
    assert!(connection.execute(sql).await.is_err());
    let mut prepared = connection.prepare(sql).await.unwrap();
    for _ in 0..2 {
        let mut rows = pin!(connection.fetch(&mut prepared));
        assert!(rows.next().await.unwrap().is_ok());
        assert!(rows.next().await.unwrap().is_err());
        assert!(rows.next().await.is_none());
    }
    let rows = connection
        .fetch(Sql().select(cols!(42)).build(&connection.driver()))
        .map_ok(|v| v.into_values().collect::<Vec<_>>())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows, [[Value::Int64(Some(42))]]);
}

#[tokio::test]
async fn outer_advance_reports_drain_errors() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let mut results = pin!(connection.run("SELECT 1 UNION ALL SELECT 2; SELECT 3"));
        let Some(Ok(QueryResult::Row(snapshot))) = results.next().await else {
            panic!("Expected row 1")
        };
        let Some(Ok(QueryResult::Row(second))) = results.next().await else {
            panic!("Expected row 2")
        };
        assert_eq!(second.value(0).unwrap().as_ref(), &Value::Int64(Some(2)));
        let Some(Ok(QueryResult::Row(third))) = results.next().await else {
            panic!("Expected row 3")
        };
        assert_eq!(third.value(0).unwrap().as_ref(), &Value::Int64(Some(3)));
        assert!(results.next().await.is_none());
        assert_eq!(snapshot.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
    }
    {
        let mut results = pin!(connection.run(
            "SELECT 1 UNION ALL SELECT abs(-9223372036854775808); CREATE TABLE not_run (id INT)"
        ));
        assert!(matches!(
            results.next().await,
            Some(Ok(QueryResult::Row(_)))
        ));
        assert!(results.next().await.unwrap().is_err());
        assert!(results.next().await.is_none());
    }
    assert!(
        connection
            .execute(
                Sql()
                    .insert_into("not_run")
                    .columns(["id"])
                    .values([[1_i64.as_value()]])
                    .build(&connection.driver()),
            )
            .await
            .is_err()
    );
    connection
        .execute("CREATE TABLE reused (id INT)")
        .await
        .unwrap();
}

#[tokio::test]
async fn partial_rows_survive_drop() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let results = connection.run("SELECT 1 UNION ALL SELECT 2; SELECT 3");
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    handle.block_on(async move {
                        let mut results = pin!(results);
                        let Some(Ok(QueryResult::Row(first))) = results.next().await else {
                            panic!("Expected row 1")
                        };
                        assert_eq!(first.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
                        let Some(Ok(QueryResult::Row(second))) = results.next().await else {
                            panic!("Expected row 2")
                        };
                        assert_eq!(second.value(0).unwrap().as_ref(), &Value::Int64(Some(2)));
                    });
                })
                .join()
                .unwrap();
        });
    }
    let mut prepared = connection
        .prepare("SELECT ? UNION ALL SELECT 2")
        .await
        .unwrap();
    for value in [10, 20] {
        prepared.bind(value).unwrap();
        let mut rows = pin!(connection.fetch(&mut prepared));
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .value(0)
                .unwrap()
                .as_ref(),
            &Value::Int64(Some(value))
        );
        // Abandon stream before the second row.
    }
    connection
        .execute("CREATE TABLE reused (id INT)")
        .await
        .unwrap();
}

#[tokio::test]
async fn abandoned_returning_finishes_write() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    connection
        .execute("CREATE TABLE items (id INT)")
        .await
        .unwrap();
    {
        let mut results = pin!(
            connection
                .run("INSERT INTO items VALUES (1), (2), (3) RETURNING id; DELETE FROM items")
        );
        assert!(matches!(
            results.next().await,
            Some(Ok(QueryResult::Row(_)))
        ));
    }
    let mut prepared = connection
        .prepare("INSERT INTO items VALUES (?) RETURNING id")
        .await
        .unwrap();
    for value in [4, 5] {
        prepared.bind(value).unwrap();
        let mut results = pin!(connection.run(&mut prepared));
        assert!(matches!(
            results.next().await,
            Some(Ok(QueryResult::Row(_)))
        ));
    }
    let ids = connection
        .fetch(
            Sql()
                .select(cols!(id))
                .from("items")
                .order_by(cols!(id ASC))
                .build(&connection.driver()),
        )
        .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(ids, [1, 2, 3, 4, 5].map(|id| Value::Int64(Some(id))));
    let mut results = pin!(
        connection
            .run("INSERT INTO items VALUES (6), (7) RETURNING id; DELETE FROM items WHERE id >= 6")
    );
    assert!(matches!(
        results.next().await,
        Some(Ok(QueryResult::Row(_)))
    ));
    assert!(matches!(
        results.next().await,
        Some(Ok(QueryResult::Row(_)))
    ));
    let Some(Ok(QueryResult::Affected(affected))) = results.next().await else {
        panic!("Expected write count")
    };
    assert_eq!(affected.rows_affected, Some(2));
    assert!(results.next().await.is_none());
}

#[tokio::test]
async fn fetch_late_prepare_errors() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let mut query = DynQuery::default();
        Sql()
            .select(cols!(1))
            .build_into(&connection.driver(), &mut query);
        Sql()
            .select(cols!(*))
            .from("missing_table")
            .build_into(&connection.driver(), &mut query);
        let mut rows = pin!(connection.fetch(query));
        assert!(rows.next().await.unwrap().is_ok());
        assert!(rows.next().await.unwrap().is_err());
        assert!(rows.next().await.is_none());
    }
    assert!(
        connection
            .run("SELECT 1\0; SELECT 2")
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    assert!(
        connection
            .execute(
                Sql()
                    .select(cols!(*))
                    .from("missing_table")
                    .build(&connection.driver()),
            )
            .await
            .is_err()
    );
    connection
        .execute(Sql().select(cols!(1)).build(&connection.driver()))
        .await
        .unwrap();
}

#[tokio::test]
async fn skipping_rows_reports_write_errors() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    connection
        .execute("CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items VALUES (1)")
        .await
        .unwrap();
    {
        let mut results =
            pin!(connection.run("INSERT INTO items VALUES (1) RETURNING id; DELETE FROM items"));
        let error = results.next().await.unwrap().err().unwrap();
        assert!(format!("{error:#}").contains("UNIQUE constraint failed"));
        assert!(results.next().await.is_none());
    }
    let rows = connection
        .fetch(
            Sql()
                .select(cols!(id))
                .from("items")
                .build(&connection.driver()),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
}

#[tokio::test]
async fn huge_select_cancels_on_drop() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::default(), "sqlite://:memory:".into())
            .await
            .unwrap();
    // A recursive query that would never finish if fully materialized on prepare
    // or on the first step. Taking a handful of rows must return promptly.
    {
        let mut rows = pin!(connection.fetch(
            "WITH RECURSIVE huge(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM huge WHERE n < 1000000000) SELECT n FROM huge",
        ));
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .value(0)
                .unwrap()
                .as_ref(),
            &Value::Int64(Some(1))
        );
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .value(0)
                .unwrap()
                .as_ref(),
            &Value::Int64(Some(2))
        );
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .value(0)
                .unwrap()
                .as_ref(),
            &Value::Int64(Some(3))
        );
        // Dropping the rowset must cancel the outstanding statement.
    }
    // The connection must be reusable once the abandoned statement is released.
    connection
        .execute("CREATE TABLE reused (id INT)")
        .await
        .unwrap();
    connection
        .execute("INSERT INTO reused VALUES (7)")
        .await
        .unwrap();
    let rows = connection
        .fetch(
            Sql()
                .select(cols!(id))
                .from("reused")
                .build(&connection.driver()),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows[0].value(0).unwrap().as_ref(), &Value::Int64(Some(7)));
}
