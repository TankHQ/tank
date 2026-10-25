#[cfg(test)]
mod tests {
    use indoc::indoc;
    use std::{path::Path, sync::Mutex};
    use tank_core::{AsValue, Connection, Executor, QueryResult, Result, Row, Value};
    use tank_duckdb::{DuckDBConnection, DuckDBDriver};
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn consumes_rows_incrementally_and_keeps_owned_snapshots() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        let first;
        {
            // Materializing this result would be infeasible. Only the requested prefix is read.
            let mut rows = connection.fetch(
                "SELECT i, 'row-' || i::VARCHAR AS label, [i, i + 1] AS nested FROM range(1000000000000) t(i)"
            ).await.unwrap();
            first = rows.next().unwrap().unwrap().into_owned();
            for i in 1..5000 {
                let row = rows.next().unwrap().unwrap();
                assert_eq!(row.value(0).unwrap().into_owned(), Value::Int64(Some(i)));
            }
        }
        assert_eq!(first.names(), &["i", "label", "nested"]);
        assert_eq!(first.value(0).unwrap().into_owned(), Value::Int64(Some(0)));
        assert_eq!(
            first.value(1).unwrap().into_owned(),
            Value::Varchar(Some("row-0".into()))
        );
        assert!(
            matches!(first.value(2).unwrap().as_ref(), Value::List(Some(values), _) if values.len() == 2 && values[1] == Value::Int64(Some(1)))
        );
        let mut rows = connection.fetch("SELECT 42").await.unwrap();
        assert_eq!(
            rows.next().unwrap().unwrap().value(0).unwrap().into_owned(),
            Value::Int32(Some(42))
        );
    }

    #[tokio::test]
    async fn late_native_fetch_error_is_observable_and_terminal() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        {
            let mut results = connection.run(
                "SELECT CAST(CASE WHEN i = 1000000 THEN 'invalid' ELSE i::VARCHAR END AS BIGINT) FROM range(1000010) t(i); CREATE TABLE must_not_run(i INT)"
            ).await.unwrap();
            let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            let mut count = 0;
            let error = loop {
                match rows.next().expect("Expected late error") {
                    Ok(_) => count += 1,
                    Err(error) => break error,
                }
            };
            assert!(
                count > 0 && count < 1000010,
                "Consumed {count} rows before error"
            );
            assert!(format!("{error:#}").contains("invalid"));
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        assert!(
            connection
                .execute("INSERT INTO must_not_run VALUES (1)")
                .await
                .is_err()
        );
        connection
            .execute("CREATE TABLE reusable(i INT)")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn late_extraction_error_is_not_hidden_by_outer_advance() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        // The native DATE is valid, but its year is outside time::Date's supported range.
        let sql = "SELECT CASE WHEN i = 5000 THEN DATE '2000000-01-01' ELSE DATE '2025-01-01' END FROM range(5001) t(i); CREATE TABLE must_not_run(i INT)";
        for drain in [false, true] {
            let mut results = connection.run(sql).await.unwrap();
            let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            assert!(rows.next().unwrap().is_ok());
            if drain {
                let error = results
                    .next()
                    .unwrap()
                    .err()
                    .expect("Drain must report extraction error");
                assert!(format!("{error:#}").contains("date"));
            } else {
                for row in rows.by_ref().take(4999) {
                    row.unwrap();
                }
                assert!(rows.next().unwrap().is_err());
            }
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        assert!(
            connection
                .execute("INSERT INTO must_not_run VALUES (1)")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn outer_advance_drains_active_rows_and_preserves_statement_order() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:".into())
                .await
                .unwrap();
        {
            let mut results = connection.run(
                "CREATE TABLE ordered(i BIGINT); INSERT INTO ordered VALUES (7); SELECT i FROM range(10000) t(i); UPDATE ordered SET i = 8; SELECT * FROM ordered; SELECT 1 WHERE false"
            ).await.unwrap();
            assert!(
                matches!(results.next().unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(0))
            );
            assert!(
                matches!(results.next().unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(1))
            );
            let QueryResult::Rows(mut old_rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            old_rows.next().unwrap().unwrap();
            assert!(
                matches!(results.next().unwrap().unwrap(), QueryResult::Affected(a) if a.rows_affected == Some(1))
            );
            assert!(old_rows.next().is_none());
            let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            assert_eq!(
                rows.next().unwrap().unwrap().value(0).unwrap().into_owned(),
                Value::Int64(Some(8))
            );
            let QueryResult::Rows(mut empty) = results.next().unwrap().unwrap() else {
                panic!("Expected empty rowset")
            };
            assert!(empty.next().is_none());
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        // Dropping an unfinished batch does not eagerly execute subsequent statements.
        {
            let mut results = connection
                .run("SELECT * FROM range(10000); INSERT INTO ordered VALUES (99)")
                .await
                .unwrap();
            let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            rows.next().unwrap().unwrap();
        }
        assert_eq!(
            connection
                .fetch("SELECT * FROM ordered")
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn draining_active_rowset_propagates_native_fetch_error() {
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        {
            let mut results = connection.run(
                "SELECT CAST(CASE WHEN i = 1000000 THEN 'invalid' ELSE i::VARCHAR END AS BIGINT) FROM range(1000010) t(i); SELECT 42"
            ).await.unwrap();
            let QueryResult::Rows(mut rows) = results.next().unwrap().unwrap() else {
                panic!("Expected rows")
            };
            rows.next().unwrap().unwrap();
            let error = results
                .next()
                .unwrap()
                .err()
                .expect("Drain must report native fetch error");
            assert!(format!("{error:#}").contains("invalid"));
            assert!(results.next().is_none());
            assert!(rows.next().is_none());
        }
        assert_eq!(
            connection
                .fetch("SELECT 42")
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn prepared_drop_reuse_and_send_iterators() {
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
            let results = connection.run(&mut query).await.unwrap();
            std::thread::scope(|scope| {
                let mut rows = scope
                    .spawn(move || {
                        let mut results = results;
                        let QueryResult::Rows(rows) = results.next().unwrap().unwrap() else {
                            panic!("Expected rows")
                        };
                        // The row iterator keeps the query and connection alive after outer drop.
                        rows
                    })
                    .join()
                    .unwrap();
                scope
                    .spawn(move || {
                        let row = rows.next().unwrap().unwrap();
                        assert_eq!(
                            row.value(0).unwrap().into_owned(),
                            Value::Varchar(Some(value.into()))
                        );
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
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .is_err()
        );
        bad.bind("42").unwrap();
        assert_eq!(
            connection
                .fetch(&mut bad)
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn statement_results_group_rows_and_distinguish_changed_counts() {
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
            .await
            .unwrap()
            .map(|result| match result.unwrap() {
                QueryResult::Rows(rows) => (None, rows.collect::<Result<Vec<_>>>().unwrap().len()),
                QueryResult::Affected(affected) => (affected.rows_affected, 0),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            &results[..7],
            &[
                (Some(0), 0),
                (Some(2), 0),
                (None, 2),
                (Some(1), 0),
                (None, 0),
                (Some(0), 0),
                (None, 1),
            ]
        );
        assert_eq!(results.len(), 8);
        assert_eq!(results[7].0, None);
        assert!(results[7].1 > 0);
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
            .await
            .unwrap()
            .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
            .collect::<Vec<_>>();
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
        assert_eq!(connection.run("-- ;\n /* ; */").await.unwrap().count(), 0);
        assert_eq!(connection.run("").await.unwrap().count(), 0);
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
        {
            let mut results = connection.run(&mut empty).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("Expected empty rowset")
            };
            assert_eq!(rows.count(), 0);
            assert!(results.next().is_none());
        }
        let mut query = connection.prepare("SELECT ? AS value").await.unwrap();
        query.bind("owned; text").unwrap();
        let rows = connection
            .fetch(&mut query)
            .await
            .unwrap()
            .map(|row| row.unwrap().into_owned())
            .collect::<Vec<_>>();
        query.bind("second").unwrap();
        assert_eq!(connection.fetch(&mut query).await.unwrap().count(), 1);
        assert_eq!(rows[0].names(), &["value"]);
        assert!(
            matches!(rows[0].value(0), Some(std::borrow::Cow::Borrowed(Value::Varchar(Some(text)))) if text == "owned; text")
        );
        assert!(
            connection
                .fetch("SELECT CAST(value AS INTEGER) FROM (VALUES ('invalid')) AS t(value)")
                .await
                .unwrap()
                .collect::<Result<Vec<_>>>()
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
        DuckDBConnection::connect(
            &DuckDBDriver::new(),
            format!("duckdb://{DB_PATH}?mode=rw").into(),
        )
        .await
        .expect("Could not open the database");
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
        let bundle = connection
            .run(indoc! {r#"
                SELECT name, value
                FROM duckdb_settings()
                WHERE name IN ('threads', 'max_memory');

            "#})
            .await
            .unwrap();
        let parameters = bundle
            .flat_map(|result| match result.unwrap() {
                QueryResult::Rows(rows) => rows,
                QueryResult::Affected(_) => panic!("Unexpected affected count"),
            })
            .collect::<Result<Vec<_>>>()
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
