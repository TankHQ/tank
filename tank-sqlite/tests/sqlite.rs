#[cfg(test)]
mod tests {
    use std::{path::Path, pin::pin, sync::Mutex};
    use tank_core::{
        Connection, ConnectionPool, Driver, Entity, Executor, QueryResult, Row, Value,
        stream::{StreamExt, TryStreamExt},
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
            .map(|result| match result.unwrap() {
                QueryResult::Row(row) => {
                    (None, Some(row.value(0).unwrap().into_owned().into_owned()))
                }
                QueryResult::Affected(affected) => (affected.rows_affected, None),
            })
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            results,
            [
                (Some(0), None),
                (Some(2), None),
                (None, Some(Value::Int64(Some(1)))),
                (None, Some(Value::Int64(Some(2)))),
                (Some(1), None),
                (Some(0), None),
                (Some(0), None),
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
            .map(|row| row.unwrap().value(0).unwrap().into_owned().into_owned())
            .collect::<Vec<_>>()
            .await;
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
                .count()
                .await,
            0
        );
        assert_eq!(connection.run("").count().await, 0);
        let mut results = pin!(connection.run("SELECT 1; SELECT * FROM missing_table"));
        assert!(matches!(results.next().await, Some(Ok(QueryResult::Row(_)))));
        assert!(results.next().await.unwrap().is_err());
        assert!(results.next().await.is_none());
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
        assert_eq!(connection.run(&mut empty).count().await, 0);
        empty.bind(2).unwrap();
        assert_eq!(connection.fetch(&mut empty).count().await, 0);
        let mut comments = connection.prepare("-- ; only a comment").await.unwrap();
        assert_eq!(connection.run(&mut comments).count().await, 0);
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
            let mut results = pin!(connection.run(&mut query));
            let Some(Ok(QueryResult::Row(row))) = results.next().await else {
                panic!("Expected RETURNING row")
            };
            assert!(results.next().await.is_none());
            vec![row.into_owned()]
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
        let values = connection
            .fetch("SELECT 1 AS id, 2 AS value UNION ALL SELECT 3, 4")
            .map(|row| row.and_then(PreparedBinding::from_row))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            values,
            [
                PreparedBinding { id: 1, value: 2 },
                PreparedBinding { id: 3, value: 4 }
            ]
        );
        assert!(
            connection
                .fetch("SELECT 1 AS id, 2 AS value WHERE false")
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            connection
                .fetch("SELECT 1 AS id, 'invalid' AS value")
                .map(|row| row.and_then(PreparedBinding::from_row))
                .try_collect::<Vec<_>>()
                .await
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
        let mut stream = pin!(connection.fetch("SELECT CAST(x'FF41' AS TEXT)"));
        let row = stream.next().await.expect("No row returned").unwrap();
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
            "Values of u64 above i64::MAX must not be silently wrapped"
        );
    }
}
