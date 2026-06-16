mod structure;

#[cfg(test)]
mod tests {
    use crate::structure::structure;
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::{ConnectionPool, Driver};
    use tank_duckdb::DuckDBDriver;
    use tank_tests::{execute_tests, init_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn duckdb() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/tests.duckdb";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH).await.expect(
                format!("Failed to remove existing test database file {}", DB_PATH).as_str(),
            );
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        let url = format!("duckdb://{}?mode=rw", DB_PATH);
        let driver = DuckDBDriver::new();
        let pool = driver
            .connect_pool(url.clone().into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        let connection = pool
            .detach()
            .await
            .expect("Could not get a connection from the pool");
        execute_tests(connection).await;

        let mut connection = pool
            .get()
            .await
            .expect("Could not get a connection from the pool");
        structure(connection.as_mut()).await;
    }
}
