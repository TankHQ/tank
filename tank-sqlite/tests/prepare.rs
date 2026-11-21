use indoc::indoc;
use tank::{Connection, Executor};
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
