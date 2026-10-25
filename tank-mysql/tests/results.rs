mod init;

use tank_core::{AsValue, Connection, Executor, QueryResult, Result, Row};
use tank_mysql::{MySQLConnection, MySQLDriver};

fn row_values(rows: impl Iterator<Item = Result<impl Row>>) -> Vec<i64> {
    rows.map(|row| i64::try_from_value(row.unwrap().value(0).unwrap()).unwrap())
        .collect()
}

fn assert_rows(
    result: Option<Result<QueryResult<impl Iterator<Item = Result<impl Row>>>>>,
    expected: &[i64],
) {
    let Some(Ok(QueryResult::Rows(rows))) = result else {
        panic!("Expected a rowset");
    };
    assert_eq!(row_values(rows), expected);
}

fn assert_affected<R>(result: Option<Result<QueryResult<R>>>, count: u64, id: Option<i64>) {
    let Some(Ok(QueryResult::Affected(affected))) = result else {
        panic!("Expected an affected count");
    };
    assert_eq!(affected.rows_affected, Some(count));
    assert_eq!(affected.last_affected_id, id);
}

#[tokio::test]
async fn raw_and_prepared_result_sets() {
    let (url, _container) = init::init_mysql(false).await;
    let mut conn = MySQLConnection::connect(&MySQLDriver::mysql(), url.into())
        .await
        .unwrap();
    assert!(conn.accepts_multiple_statements());
    conn.execute("CREATE TEMPORARY TABLE grouped_results (id INTEGER AUTO_INCREMENT PRIMARY KEY)")
        .await
        .unwrap();

    {
        let mut results = conn
            .run(
                "SELECT 1 WHERE FALSE;
                 UPDATE grouped_results SET id = 10 WHERE FALSE;
                 INSERT INTO grouped_results VALUES (NULL);
                 SELECT 2 UNION ALL SELECT 3;
                 INSERT INTO grouped_results VALUES (NULL);
                 SELECT 4;
                 SELECT 5 WHERE FALSE",
            )
            .await
            .unwrap();
        assert_rows(results.next(), &[]);
        assert_affected(results.next(), 0, None);
        assert_affected(results.next(), 1, Some(1));
        assert_rows(results.next(), &[2, 3]);
        assert_affected(results.next(), 1, Some(2));
        assert_rows(results.next(), &[4]);
        assert_rows(results.next(), &[]);
        assert!(results.next().is_none());
    }

    let mut prepared = conn
        .prepare("SELECT id FROM grouped_results WHERE id = ?")
        .await
        .unwrap();
    for (id, expected) in [(99_i64, &[][..]), (1, &[1][..])] {
        prepared.bind(id).unwrap();
        let mut results = conn.run(&mut prepared).await.unwrap();
        assert_rows(results.next(), expected);
        assert!(results.next().is_none());
    }

    let mut prepared = conn
        .prepare("UPDATE grouped_results SET id = 10 WHERE id = ?")
        .await
        .unwrap();
    prepared.bind(99_i64).unwrap();
    {
        let mut results = conn.run(prepared).await.unwrap();
        assert_affected(results.next(), 0, None);
        assert!(results.next().is_none());
    }

    let rows = conn
        .fetch("SELECT 1 WHERE FALSE; SELECT 2; UPDATE grouped_results SET id = 10 WHERE FALSE; SELECT 3")
        .await
        .unwrap();
    assert_eq!(row_values(rows), [2, 3]);

    let affected = conn
        .execute("INSERT INTO grouped_results VALUES (NULL); SELECT 1; INSERT INTO grouped_results VALUES (NULL); UPDATE grouped_results SET id = 10 WHERE FALSE")
        .await
        .unwrap();
    assert_eq!(affected.rows_affected, Some(2));
    assert_eq!(affected.last_affected_id, Some(4));

    // A later-set error must fail run itself, not appear in a returned iterator.
    assert!(
        conn.run("SELECT 1; SELECT * FROM tank_missing_grouped_results")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn raw_and_prepared_calls_preserve_native_sets() {
    let (url, _container) = init::init_mysql(false).await;
    let mut conn = MySQLConnection::connect(&MySQLDriver::mysql(), url.into())
        .await
        .unwrap();
    conn.execute("DROP PROCEDURE IF EXISTS tank_grouped_results")
        .await
        .unwrap();
    conn.execute(
        "CREATE PROCEDURE tank_grouped_results()
         BEGIN
             SELECT 1 WHERE FALSE;
             SELECT 2 UNION ALL SELECT 3;
             SELECT 4 WHERE FALSE;
         END",
    )
    .await
    .unwrap();

    {
        let mut results = conn.run("CALL tank_grouped_results()").await.unwrap();
        assert_rows(results.next(), &[]);
        assert_rows(results.next(), &[2, 3]);
        assert_rows(results.next(), &[]);
        assert_affected(results.next(), 0, None);
        assert!(results.next().is_none());
    }
    let prepared = conn.prepare("CALL tank_grouped_results()").await.unwrap();
    {
        let mut results = conn.run(prepared).await.unwrap();
        assert_rows(results.next(), &[]);
        assert_rows(results.next(), &[2, 3]);
        assert_rows(results.next(), &[]);
        assert_affected(results.next(), 0, None);
        assert!(results.next().is_none());
    }

    conn.execute("DROP PROCEDURE tank_grouped_results")
        .await
        .unwrap();
}
