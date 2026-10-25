#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};
    use tank_chdb::{ChDBConnection, ChDBDriver};
    use tank_core::{Connection, Entity, Executor, QueryResult};
    use tank_tests::{init_logs, silent_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn prepared_null_and_reuse() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        #[derive(tank::Entity, Debug, PartialEq, Clone)]
        #[tank(name = "prepared_nullable")]
        struct PreparedNullable {
            #[tank(primary_key)]
            id: u64,
            value: Option<i64>,
        }

        let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
            .await
            .expect("Could not open an in-memory database");
        PreparedNullable::drop_table(&mut connection, true, false)
            .await
            .unwrap();
        PreparedNullable::create_table(&mut connection, true, false)
            .await
            .unwrap();
        PreparedNullable::insert_many(
            &mut connection,
            [
                PreparedNullable { id: 1, value: None },
                PreparedNullable {
                    id: 2,
                    value: Some(7),
                },
            ],
        )
        .await
        .unwrap();

        // Binding NULL is a real NULL parameter: `value = NULL` matches no row.
        let mut query = PreparedNullable::prepare_find(
            &mut connection,
            tank::expr!(PreparedNullable::value == ?),
            None,
        )
        .await
        .expect("Could not prepare the query");
        query
            .bind(Option::<i64>::None)
            .expect("Could not bind a NULL");
        let rows = {
            let mut results = connection
                .run(&mut query)
                .await
                .expect("Could not query by the NULL value");
            let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
                panic!("prepared SELECT must return rows, not the internal SET directive");
            };
            assert!(results.next().is_none());
            rows.map(|row| row.and_then(PreparedNullable::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .expect("Could not query by the NULL value")
        };
        assert!(
            rows.is_empty(),
            "A NULL parameter must not match any row, got {rows:?}"
        );

        // `id != NULL` also matches nothing. Had the binding degraded to the
        // string "NULL" (or empty), this would have matched every row.
        let mut not_null = PreparedNullable::prepare_find(
            &mut connection,
            tank::expr!(PreparedNullable::id != ?),
            None,
        )
        .await
        .expect("Could not prepare the not-null query");
        not_null
            .bind(Option::<i64>::None)
            .expect("Could not bind a NULL");
        let rows = {
            let bundle = connection
                .fetch(&mut not_null)
                .await
                .expect("Could not query by the NULL id");
            bundle
                .map(|row| row.and_then(PreparedNullable::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .expect("Could not query by the NULL id")
        };
        assert!(
            rows.is_empty(),
            "A NULL parameter must not match any row, got {rows:?}"
        );

        // The same statement must be reusable with a new binding.
        query.bind(7_i64).expect("Could not rebind the value");
        let rows = {
            let bundle = connection
                .fetch(&mut query)
                .await
                .expect("Could not rerun the prepared query");
            bundle
                .map(|row| row.and_then(PreparedNullable::from_row))
                .collect::<tank_core::Result<Vec<_>>>()
                .expect("Could not rerun the prepared query")
        };
        assert_eq!(
            rows,
            [PreparedNullable {
                id: 2,
                value: Some(7)
            }]
        );
    }

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
        let bundle = connection
            .fetch("SELECT a FROM in_memory_table ORDER BY a")
            .await
            .expect("Could not query the table");
        let rows = bundle.collect::<tank_core::Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn statement_results_classify_reads_and_writes() {
        let _guard = MUTEX.lock().unwrap();
        let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
            .await
            .unwrap();
        assert!(!connection.accepts_multiple_statements());
        for sql in [
            "/* not a SQL prefix */ SELECT number FROM numbers(0)",
            "WITH 1 AS n SELECT n WHERE 0",
        ] {
            let mut results = connection.run(sql).await.unwrap();
            let Some(Ok(QueryResult::Rows(mut rows))) = results.next() else {
                panic!("empty SELECT must return rows");
            };
            assert!(rows.next().is_none());
            assert!(results.next().is_none());
        }
        for sql in [
            "CREATE TABLE statement_results (n UInt64) ENGINE = Memory",
            "INSERT INTO statement_results SELECT number FROM numbers(0)",
            "INSERT INTO statement_results VALUES (1), (2)",
        ] {
            let mut results = connection.run(sql).await.unwrap();
            let Some(Ok(QueryResult::Affected(affected))) = results.next() else {
                panic!("write must return effects");
            };
            assert_eq!(affected.rows_affected, None);
            assert!(results.next().is_none());
        }
        let mut results = connection
            .run("SELECT n FROM statement_results")
            .await
            .unwrap();
        let Some(Ok(QueryResult::Rows(rows))) = results.next() else {
            panic!("SELECT must return one rowset");
        };
        assert_eq!(
            rows.collect::<tank_core::Result<Vec<_>>>().unwrap().len(),
            2
        );
        assert!(results.next().is_none());
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
        let bundle = connection
            .fetch("SELECT a FROM persisted ORDER BY a")
            .await
            .expect("Could not query the persisted table");
        let rows = bundle.collect::<tank_core::Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 2, "Persisted table should survive reconnection");
    }
}
