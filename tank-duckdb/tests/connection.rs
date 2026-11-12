#[cfg(test)]
mod tests {
    use indoc::indoc;
    use std::{path::Path, pin::pin, sync::Mutex};
    use tank::{
        AsValue, Connection, DynQuery, Entity, Executor, QueryResult, Row, Sql, Value, cols, expr,
        stream::{StreamExt, TryStreamExt},
    };
    use tank_duckdb::{DuckDBConnection, DuckDBDriver};
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());
    const DRIVER: DuckDBDriver = DuckDBDriver::new();

    #[derive(Entity)]
    #[tank(name = "must_not_run")]
    struct MustNotRun {
        i: i32,
    }

    #[derive(Entity)]
    #[tank(name = "reusable")]
    struct Reusable {
        i: i32,
    }

    #[derive(Entity)]
    #[tank(name = "ordered")]
    struct Ordered {
        #[tank(primary_key)]
        id: i64,
        value: i64,
    }

    #[derive(Entity)]
    #[tank(name = "shared_db")]
    struct SharedDb {
        v: i32,
    }

    #[tokio::test]
    async fn incremental_owned_snapshots() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        let first;
        {
            // Materializing this result would be infeasible. Only the requested prefix is read.
            let mut rows = pin!(connection.fetch(
                "SELECT i, 'row-' || i::VARCHAR AS label, [i, i + 1] AS nested FROM range(1000000000000) t(i)"
            ));
            first = rows.next().await.unwrap().unwrap().into_owned();
            for i in 1..5000 {
                let row = rows.next().await.unwrap().unwrap();
                assert_eq!(row.value(0).unwrap().into_owned(), Value::Int64(Some(i)));
            }
        }
        assert_eq!(first.labels(), &["i", "label", "nested"]);
        assert_eq!(first.value(0).unwrap().into_owned(), Value::Int64(Some(0)));
        assert_eq!(
            first.value(1).unwrap().into_owned(),
            Value::Varchar(Some("row-0".into()))
        );
        assert!(
            matches!(first.value(2).unwrap().as_ref(), Value::List(Some(values), _) if values.len() == 2 && values[1] == Value::Int64(Some(1)))
        );
        let mut rows = pin!(connection.fetch("SELECT 42"));
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .value(0)
                .unwrap()
                .into_owned(),
            Value::Int32(Some(42))
        );
    }

    #[tokio::test]
    async fn late_fetch_error_terminal() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        {
            let mut query = DynQuery::default();
            Sql()
                .select(cols!(CAST(if range == 1000000 {
                    "invalid"
                } else {
                    range as String
                } as i64)))
                .from(expr!(range(1000010)))
                .build_into(&DRIVER, &mut query);
            Sql()
                .create_table::<MustNotRun>()
                .build_into(&DRIVER, &mut query);
            let mut results = pin!(connection.run(query));
            let mut count = 0;
            let error = loop {
                match results.next().await.expect("Expected late error") {
                    Ok(QueryResult::Row(_)) => count += 1,
                    Ok(QueryResult::Affected(_)) => panic!("Unexpected affected count"),
                    Err(error) => break error,
                }
            };
            assert!(count < 1000010, "Consumed {count} rows before error");
            assert!(format!("{error:#}").contains("invalid"));
            assert!(results.next().await.is_none());
        }
        assert!(
            connection
                .execute(
                    Sql()
                        .insert_into(MustNotRun::table())
                        .values(&[MustNotRun { i: 1 }])
                        .build(&DRIVER)
                )
                .await
                .is_err()
        );
        connection
            .execute(Sql().create_table::<Reusable>().build(&DRIVER))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn outer_advance_drains_rows() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:".into())
                .await
                .unwrap();
        {
            let mut query = DynQuery::default();
            Sql()
                .create_table::<Ordered>()
                .build_into(&DRIVER, &mut query);
            Sql()
                .insert_into(Ordered::table())
                .values(&[Ordered { id: 1, value: 7 }, Ordered { id: 2, value: 7 }])
                .build_into(&DRIVER, &mut query);
            Sql()
                .select(cols!(Ordered::value))
                .from(Ordered::table())
                .build_into(&DRIVER, &mut query);
            Sql()
                .insert_into(Ordered::table())
                .values(&[Ordered { id: 1, value: 8 }])
                .update()
                .build_into(&DRIVER, &mut query);
            Sql()
                .select(cols!(Ordered::value))
                .from(Ordered::table())
                .where_expr(expr!(Ordered::id == 1))
                .build_into(&DRIVER, &mut query);
            Sql()
                .select(cols!(Ordered::value))
                .from(Ordered::table())
                .where_expr(false)
                .build_into(&DRIVER, &mut query);
            let mut results = pin!(connection.run(query));
            assert!(
                matches!(results.next().await.unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(0))
            );
            assert!(
                matches!(results.next().await.unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(2))
            );
            assert!(matches!(
                results.next().await.unwrap().unwrap(),
                QueryResult::Row(_)
            ));
            assert!(matches!(
                results.next().await.unwrap().unwrap(),
                QueryResult::Row(_)
            ));
            assert!(
                matches!(results.next().await.unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(1))
            );
            let QueryResult::Row(row) = results.next().await.unwrap().unwrap() else {
                panic!("Expected row")
            };
            assert_eq!(row.value(0).unwrap().into_owned(), Value::Int64(Some(8)));
            assert!(results.next().await.is_none());
        }
        // Dropping an unfinished batch does not eagerly execute subsequent statements.
        {
            let mut query = DynQuery::default();
            Sql()
                .select(cols!(Ordered::value))
                .from(Ordered::table())
                .build_into(&DRIVER, &mut query);
            Sql()
                .insert_into(Ordered::table())
                .values(&[Ordered { id: 3, value: 99 }])
                .build_into(&DRIVER, &mut query);
            let mut results = pin!(connection.run(query));
            assert!(matches!(
                results.next().await.unwrap().unwrap(),
                QueryResult::Row(_)
            ));
        }
        assert_eq!(
            connection
                .fetch(
                    Sql()
                        .select(cols!(Ordered::value))
                        .from(Ordered::table())
                        .build(&DRIVER)
                )
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn drain_propagates_fetch_error() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        let mut query = DynQuery::default();
        Sql()
            .select(cols!(CAST(if range == 1000000 {
                "invalid"
            } else {
                CAST(range as String)
            } as i64)))
            .from(expr!(range(1000010)))
            .build_into(&DRIVER, &mut query);
        Sql()
            .select(cols!(42))
            .from(expr!(range(1)))
            .build_into(&DRIVER, &mut query);
        assert!(connection.execute(query).await.is_err());
        assert_eq!(
            connection
                .fetch("SELECT 42")
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn prepared_reuse_send_iterators() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:".into())
                .await
                .unwrap();
        let mut query = connection
            .prepare("SELECT ? AS value, i FROM range(10000) t(i)")
            .await
            .unwrap();
        for value in ["first", "second"] {
            query.bind(value).unwrap();
            let results = connection.run(&mut query);
            let handle = tokio::runtime::Handle::current();
            std::thread::scope(|scope| {
                scope
                    .spawn(move || {
                        handle.block_on(async move {
                            let mut results = pin!(results);
                            let QueryResult::Row(row) = results.next().await.unwrap().unwrap()
                            else {
                                panic!("Expected row")
                            };
                            assert_eq!(
                                row.value(0).unwrap().into_owned(),
                                Value::Varchar(Some(value.into()))
                            );
                        });
                    })
                    .join()
                    .unwrap();
            });
        }
        // Error cleanup also resets the prepared binding index.
        let mut bad = connection
            .prepare("SELECT CAST(? AS INTEGER)")
            .await
            .unwrap();
        bad.bind("invalid").unwrap();
        assert!(
            connection
                .fetch(&mut bad)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        bad.bind("42").unwrap();
        assert_eq!(
            connection
                .fetch(&mut bad)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn statement_results_group_rows() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:".into())
                .await
                .unwrap();
        let results = connection
            .run(
                "
            -- ; SELECT 'not a statement';
            CREATE TABLE items (id INTEGER);
            INSERT INTO items VALUES (1), (2);
            /* ; */ SELECT id FROM items ORDER BY id;
            UPDATE items SET id = id + 10 WHERE id = 1;
            SELECT id FROM items WHERE false;
            DELETE FROM items WHERE false;
            INSERT INTO items VALUES (3) RETURNING id;
            EXPLAIN SELECT * FROM items;
        ",
            )
            .map(|result| match result.unwrap() {
                QueryResult::Row(_) => None,
                QueryResult::Affected(affected) => Some(affected.rows_affected),
            })
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            &results[..7],
            &[
                Some(Some(0)),
                Some(Some(2)),
                None,
                None,
                Some(Some(1)),
                Some(Some(0)),
                None,
            ]
        );
        assert!(results.len() >= 8);
        assert!(results[7..].iter().all(Option::is_none));
        let values = connection
            .fetch(
                "
            INSERT INTO items VALUES (4);
            SELECT id FROM items ORDER BY id;
            SELECT id FROM items WHERE false;
            DELETE FROM items WHERE false;
            SELECT 99;
        ",
            )
            .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(values, [2, 3, 4, 11, 99].map(|n| Value::Int32(Some(n))));
        assert_eq!(
            connection
                .execute(
                    "
            INSERT INTO items VALUES (5), (6);
            SELECT * FROM items;
            UPDATE items SET id = id + 20 WHERE id = 5;
            DELETE FROM items WHERE false;
            CREATE TABLE after_write (id INTEGER);
        "
                )
                .await
                .unwrap()
                .rows_affected,
            Some(3)
        );
        assert_eq!(connection.run("-- ;\n /* ; */").count().await, 0);
        assert_eq!(connection.run("").count().await, 0);
    }

    #[tokio::test]
    async fn prepared_empty_and_owned_rows() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:".into())
                .await
                .unwrap();
        let mut empty = connection
            .prepare("SELECT ? AS value WHERE false")
            .await
            .unwrap();
        empty.bind("empty").unwrap();
        assert_eq!(connection.run(&mut empty).count().await, 0);
        let mut query = connection.prepare("SELECT ? AS value").await.unwrap();
        query.bind("owned; text").unwrap();
        let rows = connection
            .fetch(&mut query)
            .map(|row| (row.unwrap()).into_owned())
            .collect::<Vec<_>>()
            .await;
        query.bind("second").unwrap();
        assert_eq!(connection.fetch(&mut query).count().await, 1);
        assert_eq!(rows[0].labels(), &["value"]);
        assert!(
            matches!(rows[0].value(0), Some(std::borrow::Cow::Borrowed(Value::Varchar(Some(text)))) if text == "owned; text")
        );
        assert!(
            connection
                .fetch("SELECT CAST(value AS INTEGER) FROM (VALUES ('invalid')) AS t(value)")
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn create_database() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/creation.duckdb";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH)
                .await
                .expect(format!("Failed to remove existing test database file {DB_PATH}").as_str());
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        let mut c1 = DuckDBConnection::connect(
            &DuckDBDriver::new(),
            format!("duckdb://{DB_PATH}?mode=rw").into(),
        )
        .await
        .expect("Could not open the database");
        let mut c2 = DuckDBConnection::connect(
            &DuckDBDriver::new(),
            format!("duckdb://{DB_PATH}?mode=rw").into(),
        )
        .await
        .expect("Could not open a second connection to the same database");
        let mut query = DynQuery::default();
        Sql()
            .create_table::<SharedDb>()
            .build_into(&DRIVER, &mut query);
        Sql()
            .insert_into(SharedDb::table())
            .values(&[SharedDb { v: 7 }])
            .build_into(&DRIVER, &mut query);
        c1.execute(query).await.unwrap();
        drop(c1);
        assert_eq!(
            c2.fetch(
                Sql()
                    .select(cols!(SharedDb::v))
                    .from(SharedDb::table())
                    .build(&DRIVER)
            )
            .count()
            .await,
            1
        );
        drop(c2);
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                DuckDBConnection::connect(&DuckDBDriver::new(),"postgres://some_value".into())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn url_parameters() {
        let mut connection = DuckDBConnection::connect(
            &DuckDBDriver::new(),
            "duckdb://:memory:?threads=6&max_memory=800MiB".into(),
        )
        .await
        .expect("Could not open the database");
        let parameters = connection
            .run(indoc! {r#"
                SELECT name, value
                FROM duckdb_settings()
                WHERE name IN ('threads', 'max_memory');

            "#})
            .map_ok(|result| match result {
                QueryResult::Row(row) => (row).into_owned(),
                QueryResult::Affected(_) => panic!("Unexpected affected count"),
            })
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!parameters.is_empty());
        let threads = String::try_from_value(
            parameters
                .iter()
                .find_map(|row| {
                    if row.get_column("name").map(|value| value.into_owned())
                        == Some("threads".into())
                    {
                        row.get_column("value")
                    } else {
                        None
                    }
                })
                .expect("Could not find threads")
                .clone(),
        )
        .expect("Could not extract string");
        assert_eq!(threads, "6");
        let max_memory = String::try_from_value(
            parameters
                .iter()
                .find_map(|row| {
                    if row.get_column("name").map(|value| value.into_owned())
                        == Some("max_memory".into())
                    {
                        row.get_column("value")
                    } else {
                        None
                    }
                })
                .expect("Could not find max_memory")
                .clone(),
        )
        .expect("Could not extract string");
        assert_eq!(max_memory, "800.0 MiB");
    }
}
