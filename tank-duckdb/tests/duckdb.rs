#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use tank_core::{
        Connection, Driver, Executor, RowLabeled, TableRef, Transaction, Value,
        indoc::indoc,
        stream::{StreamExt, TryStreamExt},
    };
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
        let connection = driver
            .connect(url.clone().into())
            .await
            .expect("Could not open the database");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        execute_tests(connection).await;

        let mut connection = driver
            .connect(url.clone().into())
            .await
            .expect("Failed to connect to the DuckDB database the second time");
        let mut tx = connection
            .begin()
            .await
            .expect("Failed to start a transaction");
        tx.execute(indoc! {"
                DROP TABLE IF EXISTS users;
                CREATE TABLE users (
                    id INTEGER,
                    info STRUCT (
                        name VARCHAR,
                        age INTEGER,
                        email VARCHAR
                    )
                );
                INSERT INTO users VALUES
                    (1, {name: 'Alice', age: 30, email: 'alice@example.com'}),
                    (2, {name: 'Bob', age: 25, email: 'bob@example.com'}),
                    (3, {name: 'Charlie', age: 35, email: 'charlie@example.com'});
            "})
            .await
            .expect("Could not start a transaction");
        tx.commit().await.expect("Faield to commit the transaction");
        let users = connection
            .fetch(indoc! {"
            SELECT id, info
            FROM users
            WHERE info.age >= 30
            ORDER BY id;
        "})
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to query");
        dbg!(&users);
        let Value::Struct(Some(value), fields, table) = users[0].get_column("info").unwrap() else {
            panic!("Expected the field to be a struct containing some value");
        };
        assert_eq!(
            *fields,
            [
                ("name".to_string(), Value::Varchar(None)),
                ("age".to_string(), Value::Int32(None)),
                ("email".to_string(), Value::Varchar(None))
            ],
        );
    }
}
