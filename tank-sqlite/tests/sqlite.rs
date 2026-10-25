#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::{
        Connection, ConnectionPool, Driver, Entity, Executor, QueryResult, Row, Value,
    };
    use tank_sqlite::{SQLiteConnection, SQLiteDriver};
    use tank_tests::{PreparedBinding, execute_tests, init_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn statement_results_preserve_boundaries_and_effects() {
        let mut connection =
            SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
                .await
                .unwrap();
        let sql = "
            -- A comment containing ; SELECT 'not a statement';
            CREATE TABLE items (id INTEGER);
            INSERT INTO items VALUES (1), (2);
            /* Another ; inside a comment */
            SELECT id FROM items ORDER BY id;
            UPDATE items SET id = id + 10 WHERE id = 1;
            SELECT id FROM items WHERE false;
            DELETE FROM items WHERE false;
            CREATE TABLE after_write (id INTEGER);
        ";
        let results = connection
            .run(sql)
            .await
            .unwrap()
            .map(|result| match result.unwrap() {
                QueryResult::Rows(rows) => (
                    None,
                    rows.map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
                        .collect::<Vec<_>>(),
                ),
                QueryResult::Affected(affected) => (affected.rows_affected, vec![]),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            results,
            [
                (Some(0), vec![]),
                (Some(2), vec![]),
                (None, vec![Value::Int64(Some(1)), Value::Int64(Some(2))]),
                (Some(1), vec![]),
                (None, vec![]),
                (Some(0), vec![]),
                (Some(0), vec![]),
            ]
        );
        let rows = connection
            .fetch(
                "
            INSERT INTO items VALUES (3);
            SELECT id FROM items ORDER BY id;
            SELECT id FROM items WHERE false;
            DELETE FROM items WHERE false;
            SELECT 99;
        ",
            )
            .await
            .unwrap()
            .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(rows, [2, 3, 11, 99].map(|n| Value::Int64(Some(n))));
        let affected = connection
            .execute(
                "
            INSERT INTO items VALUES (4), (5);
            CREATE TABLE after_insert (id INTEGER);
            SELECT * FROM items;
            UPDATE items SET id = id + 20 WHERE id = 4;
            SELECT id FROM items WHERE false;
            DELETE FROM items WHERE false;
            CREATE TABLE after_update (id INTEGER);
        ",
            )
            .await
            .unwrap();
        assert_eq!(affected.rows_affected, Some(3));
        assert_eq!(
            connection
                .run("; -- only ; a comment\n /* ; */")
                .await
                .unwrap()
                .count(),
            0
        );
        assert_eq!(connection.run("").await.unwrap().count(), 0);
        let mut results = connection
            .run("SELECT 1; SELECT * FROM missing_table")
            .await
            .unwrap();
        assert!(matches!(results.next(), Some(Ok(QueryResult::Rows(_)))));
        assert!(results.next().unwrap().is_err());
        assert!(results.next().is_none());
    }

    #[tokio::test]
    async fn prepared_rows_are_grouped_and_returning_has_no_extra_effect() {
        let mut connection =
            SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
                .await
                .unwrap();
        let mut empty = connection
            .prepare("SELECT ? AS value WHERE false")
            .await
            .unwrap();
        empty.bind(1).unwrap();
        {
            let mut results = connection.run(&mut empty).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("Expected empty rowset")
            };
            assert_eq!(rows.count(), 0);
            assert!(results.next().is_none());
        }
        empty.bind(2).unwrap();
        assert_eq!(connection.fetch(&mut empty).await.unwrap().count(), 0);
        let mut comments = connection.prepare("-- ; only a comment").await.unwrap();
        assert_eq!(connection.run(&mut comments).await.unwrap().count(), 0);
        connection
            .execute("CREATE TABLE returning_items (value TEXT)")
            .await
            .unwrap();
        let mut query = connection
            .prepare("INSERT INTO returning_items VALUES (?) RETURNING value")
            .await
            .unwrap();
        query.bind("owned; text").unwrap();
        let rows = {
            let mut results = connection.run(&mut query).await.unwrap();
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("Expected RETURNING rows")
            };
            let rows = rows
                .map(|row| row.unwrap().into_owned())
                .collect::<Vec<_>>();
            assert!(results.next().is_none());
            rows
        };
        query.bind("second").unwrap();
        assert_eq!(
            connection.execute(&mut query).await.unwrap().rows_affected,
            None
        );
        assert_eq!(rows[0].names(), &["value"]);
        assert!(
            matches!(rows[0].value(0), Some(std::borrow::Cow::Borrowed(Value::Varchar(Some(text)))) if text == "owned; text")
        );
        let affected = connection
            .execute(
                "
            CREATE TABLE audit (value TEXT);
            CREATE TRIGGER item_audit AFTER INSERT ON returning_items
                BEGIN INSERT INTO audit VALUES (new.value); INSERT INTO audit VALUES (';'); END;
            INSERT INTO returning_items VALUES ('triggered');
            CREATE TABLE after_trigger (id INTEGER);
        ",
            )
            .await
            .unwrap();
        assert_eq!(affected.rows_affected, Some(1));
    }

    #[tokio::test]
    async fn ordinary_fetch_collects_entities() {
        let mut connection =
            SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
                .await
                .unwrap();
        let bundle = connection
            .fetch("SELECT 1 AS id, 2 AS value UNION ALL SELECT 3, 4")
            .await
            .unwrap();
        let values = bundle
            .map(|row| row.and_then(PreparedBinding::from_row))
            .collect::<tank_core::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            values,
            [
                PreparedBinding { id: 1, value: 2 },
                PreparedBinding { id: 3, value: 4 }
            ]
        );
        let bundle = connection
            .fetch("SELECT 1 AS id, 2 AS value WHERE false")
            .await
            .unwrap();
        assert!(
            bundle
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .unwrap()
                .is_empty()
        );
        let bundle = connection
            .fetch("SELECT 1 AS id, 'invalid' AS value")
            .await
            .unwrap();
        assert!(
            bundle
                .map(|row| row.and_then(PreparedBinding::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .is_err()
        );
    }

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
        let bundle = connection
            .fetch("SELECT CAST(x'FF41' AS TEXT)")
            .await
            .expect("Could not fetch the invalid UTF-8 text");
        let mut stream = bundle;
        let row = stream.next().expect("No row returned").unwrap();
        let text = format!("{:?}", row.value(0).unwrap());
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
