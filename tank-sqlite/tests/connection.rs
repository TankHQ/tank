#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};
    use tank_core::Connection;
    use tank_sqlite::SQLiteConnection;
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn create_database() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/creation.sqlite";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH)
                .await
                .expect(format!("Failed to remove test database file {DB_PATH}").as_str());
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        SQLiteConnection::connect(format!("sqlite://{DB_PATH}?mode=rwc").into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        SQLiteConnection::connect(format!("sqlite://{DB_PATH}?mode=ro").into())
            .await
            .expect("Could not open the database");
        fs::remove_file(DB_PATH)
            .await
            .expect(format!("Failed to remove existing test database file {DB_PATH}").as_str());
        silent_logs! {
            assert!(
                SQLiteConnection::connect(format!("sqlite://{DB_PATH}?mode=ro").into())
                    .await
                    .is_err(),
                "Should not be able to open in read only unexisting database"
            );
        }
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                SQLiteConnection::connect("duckdb://some_value".into())
                    .await
                    .is_err()
            );
        }
    }
}
