mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::{
        borrow::Cow,
        env,
        path::PathBuf,
        pin::pin,
        sync::{Arc, Mutex},
    };
    use tank_core::{
        AsValue, CollectionType, Connection, ConnectionPool, Driver, Entity, Executor, PoolConfig,
        QueryResult, Row, Transaction, Value,
        stream::{StreamExt, TryStreamExt},
    };
    use tank_postgres::{PostgresConnection, PostgresDriver};
    use tank_tests::{Author, PreparedBinding, execute_tests, init_logs, silent_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());
    static DRIVER: PostgresDriver = PostgresDriver::new();

    #[tokio::test]
    async fn simple_protocol_preserves_column_metadata_without_rows() {
        use wtx::{
            collections::VectorUsize,
            database::{
                DbClient as _, Records as _,
                client::postgres::{ClientBuffer, Config, PostgresClient, Ty},
            },
            net::Uri,
            rng::{ChaCha20, CryptoSeedableRng as _},
            tls::{PlaintextCtx, TlsConfig, TlsConnector},
        };

        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let parsed = Url::parse(&url).unwrap();
        let stream = tokio::net::TcpStream::connect((
            parsed.host_str().unwrap(),
            parsed.port().unwrap_or(5432),
        ))
        .await
        .unwrap();
        let mut rng = ChaCha20::from_std_random().unwrap();
        let buffer = ClientBuffer::new(usize::MAX, &mut rng);
        let uri = Uri::new(url.as_str());
        let config = Config::from_uri(&uri).unwrap();
        let connector = TlsConnector::new(TlsConfig::plaintext(), rng, stream, uri);
        let mut client =
            PostgresClient::<wtx::Error, _, PlaintextCtx>::connect(buffer, &config, connector)
                .await
                .unwrap();

        for _ in 0..2 {
            let mut results = VectorUsize::new();
            client
                .execute_many(
                    &mut results,
                    "SELECT 1::integer AS \"integer label\", 'payload'::text AS text_label WHERE false; \
                     SELECT 2::bigint AS bigint_label, true AS bool_label; \
                     SELECT WHERE false; \
                     SELECT FROM generate_series(1, 2)",
                    |_| Ok(()),
                )
                .await
                .unwrap();
            assert_eq!(results.len(), 4);
            assert_eq!(results[0].len(), 0);
            assert_eq!(
                results[0].columns().unwrap().collect::<Vec<_>>(),
                [("integer label", Ty::Int4), ("text_label", Ty::Text)],
            );
            assert_eq!(results[1].len(), 1);
            let columns = results[1].columns().unwrap().collect::<Vec<_>>();
            assert_eq!(
                columns,
                [("bigint_label", Ty::Int8), ("bool_label", Ty::Bool)]
            );
            assert_eq!(
                results[1].get(0).unwrap().columns().collect::<Vec<_>>(),
                columns
            );
            assert_eq!(results[2].len(), 0);
            assert_eq!(results[2].columns().unwrap().count(), 0);
            assert_eq!(results[3].len(), 2);
            assert_eq!(results[3].columns().unwrap().count(), 0);
        }
    }

    #[tokio::test]
    pub async fn postgres() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch the container");
        let mut pool = DRIVER
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);

        // SSL
        let (url, container) = init(true).await;
        let container = container.expect("Could not launch the SSL container");
        let mut pool = DRIVER
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }

    #[tokio::test]
    async fn empty_array() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url, container) = init(false).await;
        let container = container.expect("Could not launch the container");
        let pool = DRIVER
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        let mut connection = pool.get().await.expect("Could not get a connection");

        let mut bundle = pin!(connection.fetch("SELECT ARRAY[]::int[]"));
        let row = bundle.next().await.expect("No row returned").unwrap();
        assert_eq!(
            row.value(0).unwrap().into_owned(),
            Value::Array(
                Some(vec![].into()),
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 0,
                }),
            )
        );
        drop(bundle);

        drop(container);
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            let pool = DRIVER.connect_pool("mysql://some_url".into(), PoolConfig::new()).await;
            assert!(pool.is_err() || pool.unwrap().get().await.is_err());
        }
    }

    #[tokio::test]
    async fn reused_parameters_and_borrowed_results() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let mut connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();
        let mut query = connection.prepare("SELECT $1 AS value").await.unwrap();

        for value in [
            42_i32.as_value(),
            (i64::MAX - 1).as_value(),
            "longer text".as_value(),
            "short".as_value(),
            None::<i32>.as_value(),
            7_i32.as_value(),
        ] {
            query.bind(value.clone()).unwrap();
            let mut results = pin!(connection.fetch(&mut query));
            let row = results.next().await.unwrap().unwrap();
            let actual = row.value(0).unwrap();
            assert!(*actual == value || actual.is_null() && value.is_null());
        }

        // A failed local encode must not replace the caller's query handle.
        query
            .bind(Value::Unknown(Some("unsupported".into())))
            .unwrap();
        assert!(
            connection
                .fetch(&mut query)
                .map_ok(Row::into_owned)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert!(query.is_prepared());
        query.bind(9_i32).unwrap();
        {
            let mut results = pin!(connection.fetch(&mut query));
            assert_eq!(
                results.next().await.unwrap().unwrap().value(0),
                Some(Cow::Owned(9_i32.as_value()))
            );
        }

        let (names_pointer, owned, other_owned) = {
            let mut results = pin!(connection.fetch(
                "SELECT i, 'borrowed'::text AS text, '\\x0011ff'::bytea AS blob, \
                 ARRAY[['one', NULL], ['two', 'three']]::text[] AS strings \
                 FROM generate_series(1, 1000) AS i",
            ));
            let first = results.next().await.unwrap().unwrap();
            let mut count = 1;
            let mut last = None;
            while let Some(row) = results.next().await {
                count += 1;
                last = Some(row.unwrap());
            }
            let row = last.unwrap();
            assert_eq!(count, 1000);
            assert_eq!(row.value(0), Some(Cow::Owned(1000_i32.as_value())));
            assert!(row.value(row.len()).is_none());
            assert!(std::ptr::eq(row.names(), first.names()));
            assert!(matches!(
                row.value(1),
                Some(Cow::Owned(Value::Varchar(Some(Cow::Borrowed("borrowed")))))
            ));
            assert!(matches!(
                row.value(2),
                Some(Cow::Owned(Value::Blob(Some(Cow::Borrowed([0, 17, 255])))))
            ));
            {
                let Value::Array(Some(strings), ..) = row.value(3).unwrap().into_owned() else {
                    panic!("Expected a two-dimensional array");
                };
                let Value::Array(Some(first), ..) = &strings[0] else {
                    panic!("Expected the first nested array");
                };
                assert!(matches!(
                    first[0],
                    Value::Varchar(Some(Cow::Borrowed("one")))
                ));
                assert!(first[1].is_null());
            }
            let names_pointer = row.names().as_ptr();
            let owned = row.into_owned();
            assert_eq!(owned.names.as_ptr(), names_pointer);
            assert_eq!(Arc::strong_count(&owned.names), 2);
            let other_owned = first.into_owned();
            assert!(Arc::ptr_eq(&owned.names, &other_owned.names));
            (names_pointer, owned, other_owned)
        };
        let _ = (names_pointer, other_owned);
        connection.execute("SELECT 1").await.unwrap();
        assert_eq!(owned.value(1), Some(Cow::Owned("borrowed".as_value())));
        assert_eq!(
            owned.value(3),
            Some(Cow::Owned(
                [[Some("one"), None], [Some("two"), Some("three")]].as_value()
            ))
        );
        assert_eq!(
            owned.value(2),
            Some(Cow::Owned(Value::Blob(Some(vec![0, 17, 255].into()))))
        );
    }

    #[tokio::test]
    async fn native_results_preserve_rows_and_affected_counts() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let mut connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();
        assert!(!connection.accepts_multiple_statements());
        {
            let mut results = pin!(connection.run("CREATE TEMP TABLE native_results (id integer)"));
            assert!(
                matches!(results.next().await, Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(0))
            );
            assert!(results.next().await.is_none());
        }
        {
            let mut results =
                pin!(connection.run("INSERT INTO native_results VALUES (1), (2), (3)"));
            assert!(
                matches!(results.next().await, Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(3) && affected.last_affected_id.is_none())
            );
            assert!(results.next().await.is_none());
        }
        {
            let mut results = pin!(connection.run("SELECT id FROM native_results ORDER BY id"));
            let mut count = 0;
            while let Some(item) = results.next().await {
                let QueryResult::Row(row) = item.unwrap() else {
                    panic!("SELECT must yield rows");
                };
                assert_eq!(
                    row.value(0),
                    Some(Cow::Owned(((count + 1) as i32).as_value()))
                );
                count += 1;
            }
            assert_eq!(count, 3);
        }
        {
            let mut results = pin!(connection.run("SELECT id FROM native_results WHERE false"));
            assert!(results.next().await.is_none());
        }
        {
            let mut results =
                pin!(connection.run("UPDATE native_results SET id = 0 WHERE false"));
            assert!(
                matches!(results.next().await, Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(0))
            );
            assert!(results.next().await.is_none());
        }
        for (sql, expected) in [
            ("SELECT WHERE false", 0),
            ("SELECT FROM generate_series(1, 3)", 3),
            ("SELECT WHERE false", 0),
            (
                "WITH changed AS (UPDATE native_results SET id = id WHERE false RETURNING id) \
                 SELECT FROM changed",
                0,
            ),
        ] {
            let mut results = pin!(connection.run(sql));
            let mut count = 0;
            while let Some(item) = results.next().await {
                let QueryResult::Row(row) = item.unwrap() else {
                    panic!("SELECT must yield rows");
                };
                count += 1;
                assert_eq!(row.len(), 0);
                assert!(row.names().is_empty());
                assert!(row.value(0).is_none());
                assert!(row.into_owned().values.is_empty());
            }
            assert_eq!(count, expected);
        }
        for (sql, expected) in [
            (
                "UPDATE native_results SET id = id WHERE false RETURNING id",
                0,
            ),
            ("UPDATE native_results SET id = id RETURNING id", 3),
        ] {
            let mut results = pin!(connection.run(sql));
            let mut count = 0;
            while let Some(item) = results.next().await {
                let QueryResult::Row(_) = item.unwrap() else {
                    panic!("RETURNING must yield rows");
                };
                count += 1;
            }
            assert_eq!(count, expected);
        }
        assert_eq!(
            connection
                .execute("SELECT 1 WHERE false")
                .await
                .unwrap()
                .rows_affected,
            None,
        );
        assert_eq!(
            connection
                .fetch("UPDATE native_results SET id = id WHERE false")
                .count()
                .await,
            0
        );
        let affected = connection
            .execute("UPDATE native_results SET id = id + 1")
            .await
            .unwrap();
        assert_eq!(affected.rows_affected, Some(3));
    }

    #[tokio::test]
    async fn prepared_empty_rowsets_reuse_metadata_and_recover_from_errors() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let mut connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();
        let mut query = connection
            .prepare(
                "SELECT $1::text AS current_name, 7::bigint AS current_number WHERE $2::boolean",
            )
            .await
            .unwrap();
        let mut saved = Vec::new();
        for present in [false, true, false, true] {
            // Change both labels and types before each cached prepared execution.
            let previous = {
                let mut rows = pin!(connection.fetch("SELECT 1 AS previous_name"));
                rows.next().await.unwrap().unwrap().into_owned()
            };
            query.bind("retained text").unwrap().bind(present).unwrap();
            {
                let mut results = pin!(connection.run(&mut query));
                if present {
                    let Some(Ok(QueryResult::Row(row))) = results.next().await else {
                        panic!("Prepared SELECT must yield a row when present");
                    };
                    assert_eq!(row.names(), &["current_name", "current_number"]);
                    assert_eq!(row.value(0), Some(Cow::Owned("retained text".as_value())));
                    assert_eq!(row.value(1), Some(Cow::Owned(7_i64.as_value())));
                    saved.push(row.into_owned());
                }
                assert!(results.next().await.is_none());
            }
            assert_eq!(previous.names(), &["previous_name"]);
            assert_eq!(previous.value(0), Some(Cow::Owned(1_i32.as_value())));
        }
        let mut zero_columns = connection
            .prepare("SELECT WHERE $1::boolean")
            .await
            .unwrap();
        for present in [false, true, false, true] {
            zero_columns.bind(present).unwrap();
            let rows: Vec<_> = connection
                .fetch(&mut zero_columns)
                .map_ok(Row::into_owned)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(rows.len(), usize::from(present));
            assert!(
                rows.iter()
                    .all(|row| row.names().is_empty() && row.len() == 0)
            );
        }
        assert!(
            connection
                .fetch("SELECT FROM")
                .map_ok(Row::into_owned)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        let mut failing = connection
            .prepare("SELECT 10 / $1::integer AS quotient")
            .await
            .unwrap();
        failing.bind(0_i32).unwrap();
        assert!(
            connection
                .fetch(&mut failing)
                .map_ok(Row::into_owned)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert!(failing.is_prepared());
        failing.bind(2_i32).unwrap();
        let row = {
            let mut rows = pin!(connection.fetch(&mut failing));
            rows.next().await.unwrap().unwrap().into_owned()
        };
        assert_eq!(row.names(), &["quotient"]);
        assert_eq!(row.value(0), Some(Cow::Owned(5_i32.as_value())));
        assert!(
            connection
                .fetch("SELECT 1; SELECT 2")
                .map_ok(Row::into_owned)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert_eq!(connection.fetch("SELECT 1").count().await, 1);
        for row in saved {
            assert_eq!(row.names(), &["current_name", "current_number"]);
            assert_eq!(row.value(0), Some(Cow::Owned("retained text".as_value())));
            assert_eq!(row.value(1), Some(Cow::Owned(7_i64.as_value())));
        }
    }

    #[tokio::test]
    async fn postgresql_url_scheme() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url, container) = init(false).await;
        let container = container.expect("Could not launch the container");
        let url = url.replacen("postgres://", "postgresql://", 1);
        let pool = DRIVER
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to build the pool");
        pool.get()
            .await
            .expect("Failed to connect using postgresql://");
        drop(container);
    }

    #[tokio::test]
    async fn native_rows_convert_entities_and_reuse_prepared_queries() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let pool = DRIVER
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .unwrap();
        let mut connection = pool.get().await.unwrap();
        let mut query = connection
            .prepare(
                "SELECT '00000000-0000-0000-0000-000000000001'::uuid AS author_id, \
             $1::text AS name, 'UK'::text AS country, NULL::integer AS books_published \
             FROM generate_series(1, 2000)",
            )
            .await
            .unwrap();
        let mut saved = Vec::new();
        for name in ["first", "second"] {
            query.bind(name).unwrap();
            let authors: Vec<Author> = connection
                .fetch(&mut query)
                .map(|row| row.and_then(Author::from_row))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(authors.len(), 2000);
            assert!(
                authors
                    .iter()
                    .all(|a| a.name == name && a.country == "UK" && a.books_published.is_none())
            );
            saved.push(authors);
        }
        connection
            .execute("SELECT 'overwrite the read buffer'")
            .await
            .unwrap();
        assert_eq!(saved[0][1999].name, "first");
        assert_eq!(saved[1][1999].name, "second");

        {
            let empty: Vec<PreparedBinding> = connection
                .fetch("SELECT 1 AS id, 2 AS value WHERE false")
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect()
                .await
                .unwrap();
            assert!(empty.is_empty());
        }

        let error = {
            connection
                .fetch(
                    "SELECT i AS id, CASE WHEN i = 1500 THEN 'invalid' ELSE '2' END AS value \
                 FROM generate_series(1, 2000) AS i",
                )
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
        };
        assert!(format!("{error:#}").contains("invalid"));
        {
            let values: Vec<PreparedBinding> = (&mut connection)
                .fetch("SELECT 1 AS id, 3 AS value")
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(values, [PreparedBinding { id: 1, value: 3 }]);
        }

        let mut transaction = connection.begin().await.unwrap();
        assert!(!transaction.accepts_multiple_statements());
        let mut query = transaction
            .prepare("SELECT $1 AS id, $2 AS value")
            .await
            .unwrap();
        query.bind(4_i32).unwrap().bind(5_i64).unwrap();
        {
            let values: Vec<PreparedBinding> = transaction
                .fetch(&mut query)
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(values, [PreparedBinding { id: 4, value: 5 }]);
        }
        {
            let mut results = pin!(transaction.run("SELECT WHERE false"));
            assert!(results.next().await.is_none());
        }
        transaction.rollback().await.unwrap();

        // Entity conversion also works directly from a native row stream.
        {
            let mut rows = pin!(connection.fetch("SELECT 6 AS id, 7 AS value"));
            assert_eq!(
                PreparedBinding::from_row(rows.next().await.unwrap().unwrap()).unwrap(),
                PreparedBinding { id: 6, value: 7 },
            );
        }
        drop(connection);

        let mut connection = pool.get().await.unwrap();
        let values: Vec<PreparedBinding> = connection
            .fetch("SELECT 8 AS id, 9 AS value")
            .map(|row| row.and_then(PreparedBinding::from_row))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(values, [PreparedBinding { id: 8, value: 9 }]);
    }

    #[tokio::test]
    async fn check_tls() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url_full, container) = init(true).await;
        let url_full = Url::parse(&url_full).expect("Could not parse the url returned from init");
        let mut url_base = url_full.clone();
        url_base.set_query(None);
        let _container = container.expect("Could not launch container");
        let _ = PostgresConnection::connect(&PostgresDriver::new(), url_full.to_string().into())
            .await
            .expect("Connection should succeed");
        let url = url_base
            .query_pairs_mut()
            .extend_pairs(url_full.query_pairs().filter(|(k, _)| k != "sslrootcert"))
            .finish();
        let _ = PostgresConnection::connect(&PostgresDriver::new(), url.to_string().into())
            .await
            .expect_err("Connection should fail without sslrootcert");
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        unsafe {
            env::set_var(
                "PGSSLROOTCERT",
                path.join("tests/assets/root.crt").to_str().unwrap(),
            );
        }
        PostgresDriver::new()
            .connect_pool(url.to_string().into(), PoolConfig::new())
            .await
            .expect("Connection should succeed with environment variable replacing sslrootcert");
        unsafe {
            env::remove_var("PGSSLROOTCERT");
        }
        let _ = PostgresConnection::connect(&PostgresDriver::new(), url.to_string().into())
            .await
            .expect_err("Connection should fail again without sslrootcert");
    }
}
