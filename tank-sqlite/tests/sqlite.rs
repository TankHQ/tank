#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::{ConnectionPool, Driver, Executor, Value, stream::StreamExt};
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
            .connect_pool(
                format!("sqlite://{DB_PATH}?mode=rwc").into(),
                Default::default(),
            )
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

    #[tokio::test]
    async fn invalid_utf8_text_is_not_ub() {
        const DB_PATH: &'static str = "../target/debug/tests_invalid_utf8.sqlite";
        let _ = fs::remove_file(DB_PATH).await;
        let driver = SQLiteDriver::new();
        let pool = driver
            .connect_pool(
                format!("sqlite://{DB_PATH}?mode=rwc").into(),
                Default::default(),
            )
            .await
            .expect("Could not open the database");
        let mut connection = pool.get().await.expect("Could not get a connection");
        let mut stream = std::pin::pin!(
            connection.fetch(tank_core::RawQuery("SELECT CAST(x'FF41' AS TEXT)".into()))
        );
        let row = stream
            .next()
            .await
            .expect("No row returned")
            .expect("Could not read the invalid UTF-8 text");
        let text = format!("{:?}", row.values[0]);
        assert!(text.contains("\\xff") || text.contains("\\u{fffd}") || text.contains('A'));
    }

    #[tokio::test]
    async fn bind_overflowing_unsigned_is_rejected() {
        const DB_PATH: &'static str = "../target/debug/tests_bind_overflow.sqlite";
        let _ = fs::remove_file(DB_PATH).await;
        let driver = SQLiteDriver::new();
        let pool = driver
            .connect_pool(
                format!("sqlite://{DB_PATH}?mode=rwc").into(),
                Default::default(),
            )
            .await
            .expect("Could not open the database");
        let mut connection = pool.get().await.expect("Could not get a connection");
        let mut query = connection
            .prepare("SELECT ?")
            .await
            .expect("Could not prepare the query");
        assert!(
            query.bind(u64::MAX).is_err(),
            "u64 above i64::MAX must not be silently wrapped"
        );
    }
}
