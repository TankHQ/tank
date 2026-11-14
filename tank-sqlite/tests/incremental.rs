use std::{
    borrow::Cow,
    future::Future,
    pin::pin,
    task::{Context, Waker},
};
use tank::{
    AsValue, Connection, DynQuery, Entity, Executor, QueryResult, Row, Sql, Transaction, Value,
    cols,
    stream::{StreamExt, TryStreamExt},
};
use tank_sqlite::{SQLiteConnection, SQLiteDriver};
use tank_tests::init_logs;

#[tokio::test]
async fn prepared_expression_rows_bind_reuse_and_upsert() {
    #[derive(Entity, Debug, PartialEq)]
    struct Item {
        #[tank(primary_key)]
        id: i64,
        label: String,
    }

    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    Item::create_table(&mut connection, false, false)
        .await
        .unwrap();

    let row = cols!(? + 1, COALESCE(?, "unknown"));
    let sql = Sql()
        .insert_into(Item::table())
        .columns(["id", "label"])
        .values([row, row])
        .update()
        .build(&connection.driver());
    let mut prepared = connection.prepare(sql).await.unwrap();
    prepared
        .bind(0i64)
        .unwrap()
        .bind(None::<String>)
        .unwrap()
        .bind(1i64)
        .unwrap()
        .bind("second")
        .unwrap();
    assert_eq!(
        connection
            .execute(&mut prepared)
            .await
            .unwrap()
            .rows_affected,
        Some(2)
    );
    prepared
        .bind(0i64)
        .unwrap()
        .bind("updated")
        .unwrap()
        .bind(2i64)
        .unwrap()
        .bind("third")
        .unwrap();
    assert_eq!(
        connection
            .execute(&mut prepared)
            .await
            .unwrap()
            .rows_affected,
        Some(2)
    );
    let rows = Item::find_many(&mut connection, true, None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            Item {
                id: 1,
                label: "updated".into()
            },
            Item {
                id: 2,
                label: "second".into()
            },
            Item {
                id: 3,
                label: "third".into()
            },
        ]
    );
}

#[tokio::test]
async fn borrowed_and_owned_parameters_are_copied() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let mut prepared = connection.prepare("SELECT ?, ?").await.unwrap();
    for owned in [false, true] {
        for empty in [false, true] {
            let expected_text = if empty {
                String::new()
            } else {
                "héllo\0世界".repeat(32)
            };
            let expected_blob = if empty {
                Vec::new()
            } else {
                [0u8, 255, 128, 1].repeat(32)
            };
            // Drop the input buffers before executing the statement.
            {
                let text = expected_text.clone();
                let blob = expected_blob.clone();
                let (text, blob) = if owned {
                    (Cow::Owned(text), Cow::Owned(blob))
                } else {
                    (Cow::Borrowed(text.as_str()), Cow::Borrowed(blob.as_slice()))
                };
                prepared
                    .bind(Value::Varchar(Some(text)))
                    .unwrap()
                    .bind(Value::Blob(Some(blob)))
                    .unwrap();
            }
            let row = connection
                .fetch(&mut prepared)
                .try_next()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                String::try_from_value(row.value(0).unwrap()).unwrap(),
                expected_text
            );
            assert_eq!(
                row.value(1).unwrap().as_ref(),
                &Value::Blob(Some(Cow::Borrowed(&expected_blob)))
            );
        }
    }
}

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

#[tokio::test]
async fn retained_snapshots_preserve_text_blobs_and_dynamic_types() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let retained;
    {
        let mut rows = pin!(connection.fetch(
            "SELECT 'héllo' AS value, X'00FF80' AS data, NULL AS optional, 1.25 AS number \
             UNION ALL SELECT 42, X'', '', -2.5 \
             UNION ALL SELECT CAST(X'FF61' AS TEXT), NULL, 'longer string', 0.0",
        ));
        retained = rows.try_next().await.unwrap().unwrap();
        let second = rows.try_next().await.unwrap().unwrap();
        assert_eq!(second.value(0).unwrap().as_ref(), &Value::Int64(Some(42)));
        assert_eq!(
            second.value(1).unwrap().as_ref(),
            &Value::Blob(Some((&b""[..]).into()))
        );
        let third = rows.try_next().await.unwrap().unwrap();
        assert_eq!(
            String::try_from_value(third.value(0).unwrap()).unwrap(),
            "�a"
        );
        assert!(rows.try_next().await.unwrap().is_none());
        assert_eq!(f64::try_from_value(second.value(3).unwrap()).unwrap(), -2.5);
    }
    let retained = retained.into_owned();
    drop(connection);
    std::thread::spawn(move || {
        assert_eq!(retained.labels(), ["value", "data", "optional", "number"]);
        assert!(retained.value(4).is_none());
        let owned = retained.into_owned();
        let values = owned.into_values().collect::<Vec<_>>();
        assert_eq!(values[0], Value::Varchar(Some("héllo".into())));
        assert_eq!(values[1], Value::Blob(Some((&b"\x00\xff\x80"[..]).into())));
        assert_eq!(values[2], Value::Null);
        assert_eq!(values[3], Value::Float64(Some(1.25)));
    })
    .join()
    .unwrap();
}

#[tokio::test]
async fn prepared_snapshots_survive_reuse_and_schema_changes() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    connection
        .execute("CREATE TABLE items (old_name TEXT); INSERT INTO items VALUES ('first')")
        .await
        .unwrap();
    let mut prepared = connection.prepare("SELECT * FROM items").await.unwrap();
    let first = connection
        .fetch(&mut prepared)
        .try_next()
        .await
        .unwrap()
        .unwrap()
        .into_owned();
    connection.execute("ALTER TABLE items RENAME COLUMN old_name TO new_name; UPDATE items SET new_name = 'second'")
        .await.unwrap();
    for _ in 0..3 {
        let row = connection
            .fetch(&mut prepared)
            .try_next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.labels(), ["new_name"]);
        assert_eq!(
            String::try_from_value(row.value(0).unwrap()).unwrap(),
            "second"
        );
    }
    assert_eq!(first.labels(), ["old_name"]);
    assert_eq!(
        String::try_from_value(first.value(0).unwrap()).unwrap(),
        "first"
    );
}

#[tokio::test]
async fn streamed_scan_yields_without_materializing_the_result() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let mut seen = 0;
    {
        let scan = connection.fetch(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM n WHERE x < 1000) \
             SELECT x, CASE x % 3 WHEN 0 THEN NULL WHEN 1 THEN 'héllo world' ELSE '' END, CAST(x AS BLOB) FROM n",
        ).try_for_each(|row| {
            seen += 1;
            assert_eq!(i64::try_from_value(row.value(0).unwrap()).unwrap(), seen);
            let expected = match seen % 3 {
                0 => None,
                1 => Some("héllo world"),
                _ => Some(""),
            };
            assert_eq!(Option::<String>::try_from_value(row.value(1).unwrap()).unwrap().as_deref(), expected);
            assert_eq!(row.value(2).unwrap().as_ref(), &Value::Blob(Some(seen.to_string().as_bytes().into())));
            std::future::ready(Ok(()))
        });
        let mut scan = pin!(scan);
        assert!(
            scan.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        scan.await.unwrap();
    }
    assert_eq!(seen, 1000);
}

#[derive(Entity)]
struct AppendItem {
    id: i64,
}

#[tokio::test]
async fn append_is_lazy_and_cancellation_rolls_back_only_implicit_transactions() {
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    AppendItem::create_table(&mut connection, false, false)
        .await
        .unwrap();
    drop(connection.append((0..600).map(|id| AppendItem { id })));
    assert!(
        AppendItem::find_many(&mut connection, true, None)
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
    {
        let mut append = pin!(connection.append((0..600).map(|id| AppendItem { id })));
        assert!(
            append
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert!(
        AppendItem::find_many(&mut connection, true, None)
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
    {
        let mut transaction = connection.begin().await.unwrap();
        {
            let mut append = pin!(transaction.append((0..600).map(|id| AppendItem { id })));
            assert!(
                append
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        let rows = AppendItem::find_many(&mut transaction, true, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows.len(), 256);
        transaction.rollback().await.unwrap();
    }
    assert!(
        AppendItem::find_many(&mut connection, true, None)
            .try_next()
            .await
            .unwrap()
            .is_none()
    );
}
