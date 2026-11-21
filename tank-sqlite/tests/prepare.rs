use indoc::indoc;
use std::thread;
use tank::{AsValue, Connection, Executor, Row, Sql, cols, stream::TryStreamExt};
use tank_sqlite::{SQLiteConnection, SQLiteDriver};
use tank_tests::{init_logs, silent_logs};

#[tokio::test]
async fn prepare_statement() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();

    silent_logs! {
        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS a, ? AS b
                "#})
                .await
                .is_ok(),
            "Single select should be a valid statement"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    -- Symbol ? is a parameter;
                    SELECT ? AS value
                "#})
                .await
                .is_ok(),
            "Preceding single-line comment should be ignored"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    /* Symbol ? is a parameter; */
                    SELECT ? AS value
                "#})
                .await
                .is_ok(),
            "Preceding block comment should be ignored"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value -- Bound to the caller's value
                "#})
                .await
                .is_ok(),
            "Trailing line comment should be ignored"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value /* Bound to the caller's value */
                "#})
                .await
                .is_ok(),
            "Trailing block comment should be ignored"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    -- Select the value below
                    SELECT ? /* Alias the parameter */ AS value
                    -- Done
                "#})
                .await
                .is_ok(),
            "Comments around the statement should be ignored"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value;
                "#})
                .await
                .is_ok(),
            "Trailing semicolon should be accepted"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value;;
                "#})
                .await
                .is_ok(),
            "Double semicolon should be accepted"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value ; ; ;
                "#})
                .await
                .is_ok(),
            "Repeated empty statements should be accepted"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS value; -- Fetch the value;
                "#})
                .await
                .is_ok(),
            "Comment after the semicolon should be accepted"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ';' AS value
                "#})
                .await
                .is_ok(),
            "Semicolon in a string is not a separator"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ? AS "a;b"
                "#})
                .await
                .is_ok(),
            "Semicolon in an identifier is not a separator"
        );

        assert!(
            connection.prepare("").await.is_err(),
            "Empty query should be rejected"
        );

        assert!(
            connection.prepare("   \n\t ").await.is_err(),
            "Whitespace only query should be rejected"
        );

        assert!(
            connection.prepare(";").await.is_err(),
            "Empty statement should be rejected"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    -- Nothing to run, ? and ; are just text here
                "#})
                .await
                .is_err(),
            "Comment only query should be rejected"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    ;;; -- Nothing to run
                "#})
                .await
                .is_err(),
            "Empty statements should be rejected"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT 1; SELECT 2
                "#})
                .await
                .is_err(),
            "Two statements should be rejected"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT 1;
                    SELECT 2;
                "#})
                .await
                .is_err(),
            "Trailing semicolons should not allow two statements"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    ; SELECT 1; SELECT 2
                "#})
                .await
                .is_err(),
            "Preceding empty statement should not hide a second statement (which must be rejected)"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT 1;;; SELECT 2
                "#})
                .await
                .is_err(),
            "Empty statements should not hide a second statement (which must be rejected)"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT 1; -- The next select is a second statement
                    SELECT 2
                "#})
                .await
                .is_err(),
            "Comment should not hide a second statement (which must be rejected)"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT ';'; SELECT 2
                "#})
                .await
                .is_err(),
            "Literal semicolon should not hide a second statement (which must be rejected)"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    CREATE TABLE t (id INTEGER);
                    DROP TABLE t;
                "#})
                .await
                .is_err(),
            "Two statements should be rejected"
        );

        assert!(
            connection
                .prepare(indoc! {r#"
                    SELECT 1;
                    INSERT INTO missing VALUES (1);
                "#})
                .await
                .is_err(),
            "Second statement should be rejected even if it could not run"
        );

        assert!(
            connection.prepare("SELECT ? AS value").await.is_ok(),
            "Connection should stay usable after a rejection"
        );
    }
}

#[tokio::test]
async fn prepared_outlives_connection() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let sql = Sql().select(cols!(? as value)).build(&connection.driver());
    let expected = sql.as_str().into_owned();
    let mut prepared = connection.prepare(sql).await.unwrap();
    prepared.bind(41_i64).unwrap();
    // Dropping the connection while a prepared statement is still alive must defer close.
    drop(connection);
    assert!(prepared.to_string().contains(&expected));
    prepared.clear_bindings().unwrap();
    prepared.bind(99_i64).unwrap();
    drop(prepared);
}

#[tokio::test]
async fn concurrent_prepared_connection() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let mut prepared = connection
        .prepare(Sql().select(cols!(? as value)).build(&connection.driver()))
        .await
        .unwrap();
    let handle = tokio::runtime::Handle::current();
    thread::scope(|scope| {
        let stmt_thread = scope.spawn(move || {
            for i in 0..200_i64 {
                prepared.bind(i).unwrap();
                let _ = prepared.to_string();
                prepared.clear_bindings().unwrap();
            }
            drop(prepared);
        });
        let conn_thread = scope.spawn(move || {
            handle.block_on(async move {
                for i in 0..200_i64 {
                    connection
                        .execute(Sql().select(cols!(#i)).build(&connection.driver()))
                        .await
                        .unwrap();
                }
            });
        });
        stmt_thread.join().unwrap();
        conn_thread.join().unwrap();
    });
}

#[tokio::test]
async fn bind_index_overflow() {
    init_logs();
    let mut connection =
        SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
            .await
            .unwrap();
    let mut prepared = connection.prepare("SELECT ?").await.unwrap();
    assert!(prepared.bind_index(1_i64, i32::MAX as u64).is_err());
    assert!(prepared.bind_index(1_i64, u64::MAX).is_err());
    // Statement remains usable after rejecting invalid indices.
    prepared.bind_index(7_i64, 0).unwrap();
    assert!(connection.execute(&mut prepared).await.is_ok());
}

#[tokio::test]
async fn foreign_stmt_untouched() {
    init_logs();
    let mut first = SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
        .await
        .unwrap();
    let mut second = SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
        .await
        .unwrap();
    let mut prepared = first.prepare("SELECT ?").await.unwrap();
    prepared.bind(42_i64).unwrap();
    // Dropping an unpolled stream must not clear bound parameters.
    drop(first.run(&mut prepared));
    // Rejecting a statement on another connection must not clear its bindings either.
    silent_logs! {
        assert!(second.execute(&mut prepared).await.is_err());
    }
    let row = first
        .fetch(&mut prepared)
        .try_next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(i64::decode_value(row.value(0).unwrap()).unwrap(), 42);
}
