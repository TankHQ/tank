mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_clickhouse;
    use std::{pin::pin, sync::Mutex};
    use tank_clickhouse::{ClickHouseConnection, ClickHouseDriver};
    use tank_core::{
        AsValue, Connection, ConnectionPool, Driver, Entity, Executor, PoolConfig, QueryResult,
        Row,
        stream::{StreamExt, TryStreamExt},
    };
    use tank_tests::{init_logs, silent_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn prepared_null_and_reuse() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        #[derive(tank::Entity, Debug, PartialEq, Clone)]
        #[tank(schema = "testing", name = "prepared_nullable")]
        struct PreparedNullable {
            #[tank(primary_key)]
            id: u64,
            value: Option<i64>,
        }

        let (url, container) = init_clickhouse().await;
        let _container = container.expect("Could not launch the ClickHouse container");
        let mut connection = ClickHouseConnection::connect(&ClickHouseDriver::new(), url.into())
            .await
            .expect("Could not connect");
        PreparedNullable::drop_table(&mut connection, true, true)
            .await
            .unwrap();
        PreparedNullable::create_table(&mut connection, true, true)
            .await
            .unwrap();
        PreparedNullable::insert_many(
            &mut connection,
            [
                PreparedNullable { id: 1, value: None },
                PreparedNullable {
                    id: 2,
                    value: Some(7),
                },
            ],
        )
        .await
        .unwrap();

        // Binding NULL is a real NULL parameter: `value = NULL` matches no row.
        let mut query = PreparedNullable::prepare_find(
            &mut connection,
            tank::expr!(PreparedNullable::value == ?),
            None,
        )
        .await
        .expect("Could not prepare the query");
        query
            .bind(Option::<i64>::None)
            .expect("Could not bind a NULL");
        let rows: Vec<PreparedNullable> = connection
            .run(&mut query)
            .map(|result| match result? {
                QueryResult::Row(row) => PreparedNullable::from_row(row),
                QueryResult::Affected(_) => {
                    panic!("Prepared SELECT must return rows, not the internal SET directive")
                }
            })
            .try_collect()
            .await
            .expect("Could not query by the NULL value");
        assert!(rows.is_empty(), "A NULL parameter must not match any row");

        // The same statement must be reusable with a new binding.
        query.bind(7_i64).expect("Could not rebind the value");
        let rows: Vec<PreparedNullable> = connection
            .fetch(&mut query)
            .map(|row| row.and_then(PreparedNullable::from_row))
            .try_collect()
            .await
            .expect("Could not rerun the prepared query");
        assert_eq!(
            rows,
            [PreparedNullable {
                id: 2,
                value: Some(7)
            }]
        );
    }

    #[tokio::test]
    async fn url_parameters() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url, container) = init_clickhouse().await;
        let _container = container.expect("Could not launch the ClickHouse container");

        let driver = ClickHouseDriver::new();

        // The default database is used when the URL has no path.
        let base = url.split('?').next().expect("Malformed test URL");
        let base = base.trim_end_matches('/');
        let without_database = base.rsplit_once('/').expect("Malformed test URL").0;
        let pool = driver
            .connect_pool(format!("{without_database}/").into(), PoolConfig::new())
            .await
            .expect("Failed to connect without an explicit database");
        let mut connection = pool
            .get()
            .await
            .expect("Could not get a connection without an explicit database");
        let rows = connection
            .run("SELECT 1")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!rows.is_empty());

        // A non existing database must fail.
        silent_logs! {
            let result = ClickHouseConnection::connect(
                &driver,
                format!("{without_database}/this_database_does_not_exist").into(),
            )
            .await;
            assert!(
                result.is_err(),
                "Connecting to a non existing database should fail"
            );
        }
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                ClickHouseConnection::connect(
                    &ClickHouseDriver::new(),
                    "postgres://some_value".into(),
                )
                .await
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn statement_results_classify_reads_and_writes() {
        let _guard = MUTEX.lock().unwrap();
        let (url, container) = init_clickhouse().await;
        let _container = container.expect("Could not launch the ClickHouse container");
        let mut connection = ClickHouseConnection::connect(&ClickHouseDriver::new(), url.into())
            .await
            .unwrap();
        assert!(!connection.accepts_multiple_statements());
        for sql in [
            "/* not a SQL prefix */ SELECT number FROM numbers(0)",
            "WITH 1 AS n SELECT n WHERE 0",
        ] {
            let mut results = pin!(connection.run(sql));
            assert!(results.next().await.is_none());
        }
        for sql in [
            "CREATE TEMPORARY TABLE statement_results (n UInt64) ENGINE = Memory",
            "INSERT INTO statement_results SELECT number FROM numbers(0)",
            "INSERT INTO statement_results VALUES (1), (2)",
        ] {
            let mut results = pin!(connection.run(sql));
            let Some(Ok(QueryResult::Affected(affected))) = results.next().await else {
                panic!("Write must return effects");
            };
            assert_eq!(affected.rows_affected, None);
            assert!(results.next().await.is_none());
        }
        // Multiple native blocks still belong to a single statement result.
        let rows = connection
            .run("SELECT number FROM numbers(5) SETTINGS max_block_size = 1")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|r| matches!(r, QueryResult::Row(_))));
    }

    #[tokio::test]
    async fn connection_settings() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url, container) = init_clickhouse().await;
        let _container = container.expect("Could not launch the ClickHouse container");

        let mut connection = ClickHouseConnection::connect(&ClickHouseDriver::new(), url.into())
            .await
            .expect("Could not connect");

        // The driver sets these session settings on connect.
        for (name, expected) in [("join_use_nulls", "1"), ("final", "1")] {
            let mut bundle = pin!(connection.fetch(format!(
                "SELECT value FROM system.settings WHERE name = '{name}'"
            )));
            let row = bundle
                .next()
                .await
                .expect("Could not find the session setting row")
                .expect("Could not read the session setting row");
            let value = row
                .get_column("value")
                .expect("Could not find the session setting value");
            assert_eq!(
                String::try_from_value(value).as_deref().ok(),
                Some(expected),
                "Unexpected value for the '{name}' setting"
            );
        }
    }
}
