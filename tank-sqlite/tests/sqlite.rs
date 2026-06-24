#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::{ConnectionPool, Driver};
    use tank_sqlite::SQLiteDriver;
    use tank_tests::{execute_tests, init_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn sqlite() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/tests.sqlite";
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
        let driver = SQLiteDriver::new();
        let mut pool = driver
            .connect_pool(format!("sqlite://{DB_PATH}?mode=rwc").into())
            .await
            .expect("Could not open the database");
        let connection = pool
            .get()
            .await
            .expect("Could not get a SQLite connection from the pool");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        drop(connection);
        execute_tests(&mut pool).await;
    }
}
