mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::{
        borrow::Cow,
        env,
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    use tank_core::{
        AsValue, Connection, ConnectionPool, Driver, Entity, Executor, PoolConfig, QueryResult,
        Row, Transaction, Value,
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

        let mut bundle = connection
            .fetch("SELECT ARRAY[]::int[]")
            .await
            .expect("Could not fetch the empty array");
        let row = bundle.next().expect("No row returned").unwrap();
        assert_eq!(
            row.value(0).unwrap().into_owned(),
            Value::Array(Some(vec![].into()), Box::new(Value::Int32(None)), 0)
        );

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
            let mut results = connection.fetch(&mut query).await.unwrap();
            let row = results.next().unwrap().unwrap();
            let actual = row.value(0).unwrap();
            assert!(*actual == value || actual.is_null() && value.is_null());
        }

        // A failed local encode must not replace the caller's query handle.
        query
            .bind(Value::Unknown(Some("unsupported".into())))
            .unwrap();
        assert!(connection.fetch(&mut query).await.is_err());
        assert!(query.is_prepared());
        query.bind(9_i32).unwrap();
        let mut results = connection.fetch(&mut query).await.unwrap();
        assert_eq!(
            results.next().unwrap().unwrap().value(0),
            Some(Cow::Owned(9_i32.as_value()))
        );
        drop(results);

        let mut results = connection
            .fetch(
                "SELECT i, 'borrowed'::text AS text, '\\x0011ff'::bytea AS blob, \
             ARRAY[['one', NULL], ['two', 'three']]::text[] AS strings \
             FROM generate_series(1, 1000) AS i",
            )
            .await
            .unwrap();
        let first = results.next().unwrap().unwrap();
        let mut count = 1;
        let row = results
            .map(Result::unwrap)
            .inspect(|_| count += 1)
            .last()
            .unwrap();
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
            let Value::Array(Some(strings), _, 2) = row.value(3).unwrap().into_owned() else {
                panic!("Expected a two-dimensional array");
            };
            let Value::Array(Some(first), _, 2) = &strings[0] else {
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
            let mut results = connection
                .run("CREATE TEMP TABLE native_results (id integer)")
                .await
                .unwrap();
            assert!(
                matches!(results.next(), Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(0))
            );
            assert!(results.next().is_none());
        }
        {
            let mut results = connection
                .run("INSERT INTO native_results VALUES (1), (2), (3)")
                .await
                .unwrap();
            assert!(
                matches!(results.next(), Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(3) && affected.last_affected_id.is_none())
            );
            assert!(results.next().is_none());
        }
        {
            let mut results = connection
                .run("SELECT id FROM native_results ORDER BY id")
                .await
                .unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("SELECT must yield one rowset");
            };
            let mut count = 0;
            for (index, row) in rows.map(Result::unwrap).enumerate() {
                count += 1;
                assert_eq!(
                    row.value(0),
                    Some(Cow::Owned(((index + 1) as i32).as_value()))
                );
            }
            assert_eq!(count, 3);
            assert!(results.next().is_none());
        }
        {
            let mut results = connection
                .run("SELECT id FROM native_results WHERE false")
                .await
                .unwrap();
            let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
                panic!("An empty SELECT must yield an empty rowset");
            };
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        {
            let mut results = connection
                .run("UPDATE native_results SET id = 0 WHERE false")
                .await
                .unwrap();
            assert!(
                matches!(results.next(), Some(Ok(QueryResult::Affected(affected)))
                if affected.rows_affected == Some(0))
            );
            assert!(results.next().is_none());
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
            let mut results = connection.run(sql).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("A zero-column SELECT must still yield a rowset");
            };
            let mut count = 0;
            for row in rows.map(Result::unwrap) {
                count += 1;
                assert_eq!(row.len(), 0);
                assert!(row.names().is_empty());
                assert!(row.value(0).is_none());
                assert!(row.into_owned().values.is_empty());
            }
            assert_eq!(count, expected);
            assert!(results.next().is_none());
        }
        for (sql, expected) in [
            (
                "UPDATE native_results SET id = id WHERE false RETURNING id",
                0,
            ),
            ("UPDATE native_results SET id = id RETURNING id", 3),
        ] {
            let mut results = connection.run(sql).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("RETURNING must yield a rowset, even when empty");
            };
            assert_eq!(rows.map(Result::unwrap).count(), expected);
            assert!(results.next().is_none());
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
                .await
                .unwrap()
                .count(),
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
            let previous = connection
                .fetch("SELECT 1 AS previous_name")
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .into_owned();
            query.bind("retained text").unwrap().bind(present).unwrap();
            let mut results = connection.run(&mut query).await.unwrap();
            let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
                panic!("Prepared SELECT must retain rowset metadata when empty");
            };
            if present {
                let row = rows.next().unwrap().unwrap();
                assert_eq!(row.names(), &["current_name", "current_number"]);
                assert_eq!(row.value(0), Some(Cow::Owned("retained text".as_value())));
                assert_eq!(row.value(1), Some(Cow::Owned(7_i64.as_value())));
                saved.push(row.into_owned());
            }
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
            assert_eq!(previous.names(), &["previous_name"]);
            assert_eq!(previous.value(0), Some(Cow::Owned(1_i32.as_value())));
        }
        let mut zero_columns = connection
            .prepare("SELECT WHERE $1::boolean")
            .await
            .unwrap();
        for present in [false, true, false, true] {
            zero_columns.bind(present).unwrap();
            let mut results = connection.run(&mut zero_columns).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("Prepared zero-column SELECT must yield a rowset");
            };
            let rows: Vec<_> = rows.map(|row| row.unwrap().into_owned()).collect();
            assert_eq!(rows.len(), usize::from(present));
            assert!(
                rows.iter()
                    .all(|row| row.names().is_empty() && row.len() == 0)
            );
            assert!(results.next().is_none());
        }
        assert!(connection.run("SELECT FROM").await.is_err());
        let mut failing = connection
            .prepare("SELECT 10 / $1::integer AS quotient")
            .await
            .unwrap();
        failing.bind(0_i32).unwrap();
        assert!(connection.run(&mut failing).await.is_err());
        assert!(failing.is_prepared());
        failing.bind(2_i32).unwrap();
        let row = connection
            .fetch(&mut failing)
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .into_owned();
        assert_eq!(row.names(), &["quotient"]);
        assert_eq!(row.value(0), Some(Cow::Owned(5_i32.as_value())));
        assert!(connection.run("SELECT 1; SELECT 2").await.is_err());
        assert_eq!(connection.fetch("SELECT 1").await.unwrap().count(), 1);
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
            let results = connection.fetch(&mut query).await.unwrap();
            let authors: Vec<Author> = results
                .map(|row| row.and_then(Author::from_row))
                .collect::<tank_core::Result<_>>()
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
            let results = connection
                .fetch("SELECT 1 AS id, 2 AS value WHERE false")
                .await
                .unwrap();
            let empty: Vec<PreparedBinding> = results
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<_>>()
                .unwrap();
            assert!(empty.is_empty());
        }

        let error = {
            let results = connection
                .fetch(
                    "SELECT i AS id, CASE WHEN i = 1500 THEN 'invalid' ELSE '2' END AS value \
                 FROM generate_series(1, 2000) AS i",
                )
                .await
                .unwrap();
            results
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .unwrap_err()
        };
        assert!(format!("{error:#}").contains("invalid"));
        {
            let results = (&mut connection)
                .fetch("SELECT 1 AS id, 3 AS value")
                .await
                .unwrap();
            let values: Vec<PreparedBinding> = results
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<_>>()
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
            let results = transaction.fetch(&mut query).await.unwrap();
            let values: Vec<PreparedBinding> = results
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<_>>()
                .unwrap();
            assert_eq!(values, [PreparedBinding { id: 4, value: 5 }]);
        }
        {
            let mut results = transaction.run("SELECT WHERE false").await.unwrap();
            let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
                panic!("Transaction must preserve an empty zero-column rowset");
            };
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        transaction.rollback().await.unwrap();

        // Entity conversion also works directly from a native row iterator.
        let mut rows = connection
            .fetch("SELECT 6 AS id, 7 AS value")
            .await
            .unwrap();
        assert_eq!(
            PreparedBinding::from_row(rows.next().unwrap().unwrap()).unwrap(),
            PreparedBinding { id: 6, value: 7 },
        );
        drop(rows);
        drop(connection);

        let mut connection = pool.get().await.unwrap();
        let results = connection
            .fetch("SELECT 8 AS id, 9 AS value")
            .await
            .unwrap();
        let values: Vec<PreparedBinding> = results
            .map(|row| row.and_then(PreparedBinding::from_row))
            .collect::<tank_core::Result<_>>()
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
