#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};
    use tank_core::{
        AsValue, Connection, Executor, QueryResult, indoc::indoc, stream::TryStreamExt,
    };
    use tank_duckdb::DuckDBConnection;
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn create_database() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/creation.duckdb";
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
        DuckDBConnection::connect(format!("duckdb://{DB_PATH}?mode=rw").into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                DuckDBConnection::connect("postgres://some_value".into())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn url_parameters() {
        let mut connection =
            DuckDBConnection::connect("duckdb://:memory:?threads=6&max_memory=800MiB".into())
                .await
                .expect("Could not open the database");
        let parameters = connection
            .run(indoc! {r#"
                SELECT name, value
                FROM duckdb_settings()
                WHERE name IN ('threads', 'max_memory');

            "#})
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not get the query result");
        assert!(!parameters.is_empty());
        let threads = String::try_from_value(
            parameters
                .iter()
                .find_map(|v| match v {
                    QueryResult::Row(row) => {
                        if row.get_column("name") == Some(&"threads".into()) {
                            row.get_column("value")
                        } else {
                            None
                        }
                    }
                    _ => panic!("Unexpected result type"),
                })
                .expect("Could not find threads")
                .clone(),
        )
        .expect("Could not extract string");
        assert_eq!(threads, "6");
        let max_memory = String::try_from_value(
            parameters
                .iter()
                .find_map(|v| match v {
                    QueryResult::Row(row) => {
                        if row.get_column("name") == Some(&"max_memory".into()) {
                            row.get_column("value")
                        } else {
                            None
                        }
                    }
                    _ => panic!("Unexpected result type"),
                })
                .expect("Could not find max_memory")
                .clone(),
        )
        .expect("Could not extract string");
        assert_eq!(max_memory, "800.0 MiB");
    }
}
