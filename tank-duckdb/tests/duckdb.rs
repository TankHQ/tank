#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::Driver;
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
        let driver = DuckDBDriver::new();
        let connection = driver
            .connect(format!("duckdb://{}?mode=rw", DB_PATH).into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        execute_tests(connection).await;
    }
}
