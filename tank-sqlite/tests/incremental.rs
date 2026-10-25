use tank_core::{Connection, Executor, QueryResult, Row, Value};
use tank_sqlite::{SQLiteConnection, SQLiteDriver};

#[tokio::test]
async fn late_step_errors_are_fallible_and_connection_is_reusable() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let sql = "SELECT 1 UNION ALL SELECT abs(-9223372036854775808)";
    {
        let mut rows = connection.fetch(sql).await.unwrap();
        let first = rows.next().unwrap().unwrap();
        assert_eq!(first.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
        let error = rows.next().unwrap().unwrap_err();
        assert!(format!("{error:#}").contains("integer overflow"));
        assert!(rows.next().is_none());
        assert_eq!(first.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
    }
    assert!(connection.execute(sql).await.is_err());
    let mut prepared = connection.prepare(sql).await.unwrap();
    for _ in 0..2 {
        let mut rows = connection.fetch(&mut prepared).await.unwrap();
        assert!(rows.next().unwrap().is_ok());
        assert!(rows.next().unwrap().is_err());
        assert!(rows.next().is_none());
    }
    let rows = connection
        .fetch("SELECT 42")
        .await
        .unwrap()
        .collect::<tank_core::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows[0].value(0).unwrap().as_ref(), &Value::Int64(Some(42)));
}

#[tokio::test]
async fn outer_advance_drains_active_rows_and_reports_drain_errors() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let mut results = connection
            .run("SELECT 1 UNION ALL SELECT 2; SELECT 3")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Rows(mut first))) = results.next() else {
            panic!("Expected rows")
        };
        let snapshot = first.next().unwrap().unwrap();
        let Some(Ok(QueryResult::Rows(mut second))) = results.next() else {
            panic!("Expected rows")
        };
        assert!(first.next().is_none());
        assert_eq!(
            second.next().unwrap().unwrap().value(0).unwrap().as_ref(),
            &Value::Int64(Some(3))
        );
        assert!(results.next().is_none());
        assert!(second.next().is_none());
        assert_eq!(snapshot.value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
    }
    {
        let mut results = connection.run(
            "SELECT 1 UNION ALL SELECT abs(-9223372036854775808); CREATE TABLE not_run (id INT)"
        ).await.unwrap();
        let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
            panic!("Expected rows")
        };
        assert!(rows.next().unwrap().is_ok());
        assert!(results.next().unwrap().is_err());
        assert!(rows.next().is_none());
        assert!(results.next().is_none());
    }
    assert!(
        connection
            .execute("INSERT INTO not_run VALUES (1)")
            .await
            .is_err()
    );
    connection
        .execute("CREATE TABLE reused (id INT)")
        .await
        .unwrap();
}

#[tokio::test]
async fn partial_rows_survive_outer_drop_and_move_between_threads() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let mut results = connection
            .run("SELECT 1 UNION ALL SELECT 2; SELECT 3")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
            panic!("Expected rows")
        };
        drop(results);
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    assert_eq!(
                        rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
                        &Value::Int64(Some(1))
                    );
                    assert_eq!(
                        rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
                        &Value::Int64(Some(2))
                    );
                    assert!(rows.next().is_none());
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
        let mut rows = connection.fetch(&mut prepared).await.unwrap();
        assert_eq!(
            rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
            &Value::Int64(Some(value))
        );
        // Abandon both iterator levels before the second row.
    }
    connection
        .execute("CREATE TABLE reused (id INT)")
        .await
        .unwrap();
}

#[tokio::test]
async fn abandoned_returning_finishes_write_but_not_later_statements() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    connection
        .execute("CREATE TABLE items (id INT)")
        .await
        .unwrap();
    {
        let mut results = connection
            .run("INSERT INTO items VALUES (1), (2), (3) RETURNING id; DELETE FROM items")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
            panic!("Expected rows")
        };
        assert!(rows.next().unwrap().is_ok());
    }
    let mut prepared = connection
        .prepare("INSERT INTO items VALUES (?) RETURNING id")
        .await
        .unwrap();
    for value in [4, 5] {
        prepared.bind(value).unwrap();
        let mut results = connection.run(&mut prepared).await.unwrap();
        // Even an unstepped, published RETURNING rowset must finish on drop.
        assert!(matches!(results.next(), Some(Ok(QueryResult::Rows(_)))));
    }
    let ids = connection
        .fetch("SELECT id FROM items ORDER BY id")
        .await
        .unwrap()
        .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 2, 3, 4, 5].map(|id| Value::Int64(Some(id))));
    // Advancing the outer iterator also completes RETURNING before the next write.
    let mut results = connection
        .run("INSERT INTO items VALUES (6), (7) RETURNING id; DELETE FROM items WHERE id >= 6")
        .await
        .unwrap();
    let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
        panic!("Expected rows")
    };
    let Some(Ok(QueryResult::Affected(affected))) = results.next() else {
        panic!("Expected write count")
    };
    assert_eq!(affected.rows_affected, Some(2));
    assert!(rows.next().is_none());
    assert!(results.next().is_none());
}

#[tokio::test]
async fn fetch_reports_late_prepare_errors_and_rejects_interior_nul() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    {
        let mut rows = connection
            .fetch("SELECT 1; SELECT * FROM missing_table")
            .await
            .unwrap();
        assert!(rows.next().unwrap().is_ok());
        assert!(rows.next().unwrap().is_err());
        assert!(rows.next().is_none());
    }
    assert!(connection.run("SELECT 1\0; SELECT 2").await.is_err());
    assert!(
        connection
            .execute("SELECT * FROM missing_table")
            .await
            .is_err()
    );
    connection.execute("SELECT 1").await.unwrap();
}

#[tokio::test]
async fn skipping_returning_rows_reports_write_errors_on_outer_advance() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    connection
        .execute("CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items VALUES (1)")
        .await
        .unwrap();
    {
        let mut results = connection
            .run("INSERT INTO items VALUES (1) RETURNING id; DELETE FROM items")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
            panic!("Expected rows")
        };
        drop(rows);
        let error = results.next().unwrap().err().unwrap();
        assert!(format!("{error:#}").contains("UNIQUE constraint failed"));
        assert!(results.next().is_none());
    }
    let rows = connection
        .fetch("SELECT id FROM items")
        .await
        .unwrap()
        .collect::<tank_core::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value(0).unwrap().as_ref(), &Value::Int64(Some(1)));
}

#[tokio::test]
async fn huge_select_steps_only_on_demand_and_drop_cancels_it() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::default(), "sqlite://:memory:".into())
            .await
            .unwrap();
    // A recursive query that would never finish if fully materialized on prepare
    // or on the first step. Taking a handful of rows must return promptly.
    {
        let mut rows = connection
            .fetch(
                "WITH RECURSIVE huge(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM huge WHERE n < 1000000000) SELECT n FROM huge",
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
            &Value::Int64(Some(1))
        );
        assert_eq!(
            rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
            &Value::Int64(Some(2))
        );
        assert_eq!(
            rows.next().unwrap().unwrap().value(0).unwrap().as_ref(),
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
        .fetch("SELECT id FROM reused")
        .await
        .unwrap()
        .collect::<tank_core::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows[0].value(0).unwrap().as_ref(), &Value::Int64(Some(7)));
}
