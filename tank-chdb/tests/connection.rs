#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};
    use tank_chdb::{ChDBConnection, ChDBDriver};
    use tank_core::{Connection, Executor, stream::TryStreamExt};
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn create_database() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/creation.chdb";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_dir_all(DB_PATH)
                .await
                .expect(format!("Failed to remove existing test database at {DB_PATH}").as_str());
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database should not exist before test"
        );
        ChDBConnection::connect(&ChDBDriver::new(), format!("chdb://?path={DB_PATH}").into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database should be created after connection"
        );
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                ChDBConnection::connect(&ChDBDriver::new(), "duckdb://some_value".into())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn in_memory() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
            .await
            .expect("Could not open an in-memory database");
        connection
            .execute("CREATE TABLE in_memory_table (a UInt8) ENGINE = Memory")
            .await
            .expect("Could not create the table");
        connection
            .execute("INSERT INTO in_memory_table VALUES (1), (2)")
            .await
            .expect("Could not insert the rows");
        let rows = connection
            .fetch("SELECT a FROM in_memory_table ORDER BY a")
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not query the table");
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn url_parameters() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/parameters.chdb";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_dir_all(DB_PATH)
                .await
                .expect(format!("Failed to remove existing test database at {DB_PATH}").as_str());
        }
        let mut connection =
            ChDBConnection::connect(&ChDBDriver::new(), format!("chdb://?path={DB_PATH}").into())
                .await
                .expect("Could not open the database");
        connection
            .execute("CREATE TABLE persisted (a UInt8) ENGINE = MergeTree ORDER BY a")
            .await
            .expect("Could not create the table");
        connection
            .execute("INSERT INTO persisted VALUES (7), (8)")
            .await
            .expect("Could not insert the rows");
        drop(connection);

        let mut connection =
            ChDBConnection::connect(&ChDBDriver::new(), format!("chdb://?path={DB_PATH}").into())
                .await
                .expect("Could not reopen the database");
        let rows = connection
            .fetch("SELECT a FROM persisted ORDER BY a")
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not query the persisted table");
        assert_eq!(rows.len(), 2, "Persisted table should survive reconnection");
    }
}
