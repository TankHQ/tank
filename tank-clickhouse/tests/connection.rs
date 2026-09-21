mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_clickhouse;
    use std::sync::Mutex;
    use tank_clickhouse::{ClickHouseConnection, ClickHouseDriver};
    use tank_core::{
        AsValue, Connection, ConnectionPool, Driver, Executor, PoolConfig, QueryResult,
        stream::TryStreamExt,
    };
    use tank_tests::{init_logs, silent_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

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
            .expect("Could not run a query on the default database");
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
            let rows = connection
                .run(format!(
                    "SELECT value FROM system.settings WHERE name = '{name}'"
                ))
                .try_collect::<Vec<_>>()
                .await
                .expect("Could not read the session setting");
            let value = rows
                .iter()
                .find_map(|v| match v {
                    QueryResult::Row(row) => row.get_column("value").cloned(),
                    _ => None,
                })
                .expect("Could not find the session setting value");
            assert_eq!(
                String::try_from_value(value).as_deref().ok(),
                Some(expected),
                "Unexpected value for the '{name}' setting"
            );
        }
    }
}
