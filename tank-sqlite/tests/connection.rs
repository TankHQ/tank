#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};
    use tank_core::{
        AsValue, ColumnDef, Connection, Executor, Row, Sql, describe_path, stream::TryStreamExt,
    };
    use tank_sqlite::{SQLiteConnection, SQLiteDriver};
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());
    const DRIVER: SQLiteDriver = SQLiteDriver::new();

    #[tokio::test]
    async fn create_database() {
        init_logs();

        // Setup
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

        // Opening in mode=rwc creates a db file
        {
            SQLiteConnection::connect(&DRIVER, format!("sqlite://{DB_PATH}?mode=rwc").into())
                .await
                .expect("Could not open the database");
            assert!(
                Path::new(DB_PATH).exists(),
                "Database file should be created after connection"
            );
        }

        // Only mode=ro can open read only file
        {
            let mut permissions = fs::metadata(DB_PATH)
                .await
                .expect("Failed to get the file metadata")
                .permissions();
            permissions.set_readonly(true);
            fs::set_permissions(DB_PATH, permissions)
                .await
                .expect("Failed to set file permissions");
            SQLiteConnection::connect(&DRIVER, format!("sqlite://{DB_PATH}?mode=ro").into())
                .await
                .expect("Could not open the database");
            if let Ok(mut connection) =
                SQLiteConnection::connect(&DRIVER, format!("sqlite://{DB_PATH}?mode=rwc").into())
                    .await
            {
                assert!(
                    connection
                        .execute(
                            Sql()
                                .create_table("this_is_a_test_table")
                                .columns([ColumnDef::from_type::<i32>("id")])
                                .build(&DRIVER)
                        )
                        .await
                        .is_err()
                )
            }
        }
        {
            fs::remove_file(DB_PATH)
                .await
                .expect(format!("Failed to remove existing test database file {DB_PATH}").as_str());
            silent_logs! {
                assert!(
                    SQLiteConnection::connect(
                        &DRIVER,
                        format!("sqlite://{DB_PATH}?mode=ro").into(),
                    )
                    .await
                    .is_err(),
                    "Should not be able to open in read only unexisting database"
                );
            }
        }
    }

    #[tokio::test]
    async fn read_only_rejects_writes() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/read_only.sqlite";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH)
                .await
                .expect("Failed to remove the read-only test database");
        }
        let driver = DRIVER;
        {
            let mut connection =
                SQLiteConnection::connect(&driver, format!("sqlite://{DB_PATH}?mode=rwc").into())
                    .await
                    .expect("Could not create the read-only test database");
            let create_table = Sql()
                .create_table("t")
                .if_not_exists()
                .columns([
                    ColumnDef::from_type::<i32>("id").primary_key(),
                    ColumnDef::from_type::<String>("value"),
                ])
                .build(&driver);
            connection
                .execute(create_table)
                .await
                .expect("Could not create the test table");
            let insert = Sql()
                .insert_into("t")
                .columns(["value"])
                .values([["before"]])
                .build(&driver);
            connection
                .execute(insert)
                .await
                .expect("Could not insert into the test table");
        }
        {
            let mut connection =
                SQLiteConnection::connect(&driver, format!("sqlite://{DB_PATH}?mode=ro").into())
                    .await
                    .expect("Could not open the database read-only");
            let insert = Sql()
                .insert_into("t")
                .columns(["value"])
                .values([["after"]])
                .build(&driver);
            let error = connection
                .execute(insert)
                .await
                .expect_err("Writing through a read-only connection must fail");
            assert!(
                format!("{error:#}").to_lowercase().contains("readonly"),
                "Unexpected read-only error: {error:#}"
            );
            let create_table = Sql()
                .create_table("other")
                .columns([ColumnDef::from_type::<i32>("id")])
                .build(&driver);
            assert!(
                connection.execute(create_table).await.is_err(),
                "Creating a table through a read-only connection must fail"
            );
        }
        fs::remove_file(DB_PATH)
            .await
            .expect("Failed to remove the read-only test database");
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                SQLiteConnection::connect(&DRIVER, "duckdb://some_value".into())
                    .await
                    .is_err()
            );
        }
    }

    #[test]
    fn describe_paths() {
        init_logs();
        let memory = Url::parse("sqlite://localhost?mode=memory").unwrap();
        assert_eq!(describe_path(&memory), ":memory:");

        let explicit = Url::parse("sqlite://localhost?path=/tmp/x.db").unwrap();
        assert_eq!(describe_path(&explicit), "/tmp/x.db");

        let path = Url::parse("sqlite:///tmp/y.db").unwrap();
        assert!(describe_path(&path).ends_with("/tmp/y.db"));

        let bare = Url::parse("sqlite://").unwrap();
        assert_eq!(describe_path(&bare), "N/A");
    }

    #[tokio::test]
    async fn busy_timeout_option() {
        init_logs();
        for (url, expected) in [
            ("sqlite://:memory:", 5_000_i64),
            ("sqlite://:memory:?busy_timeout=250", 250_i64),
            ("sqlite://:memory:?busy_timeout=0", 0_i64),
        ] {
            let mut connection = SQLiteConnection::connect(&DRIVER, url.into())
                .await
                .unwrap();
            let row = connection
                .fetch("PRAGMA busy_timeout")
                .try_next()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(i64::decode_value(row.value(0).unwrap()).unwrap(), expected);
        }
        silent_logs! {
            for invalid in [
                "sqlite://:memory:?busy_timeout=-1",
                "sqlite://:memory:?busy_timeout=abc",
            ] {
                assert!(SQLiteConnection::connect(&DRIVER, invalid.into()).await.is_err());
            }
        }
    }
}
