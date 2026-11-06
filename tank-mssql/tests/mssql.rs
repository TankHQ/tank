mod comments;
mod init;

#[cfg(test)]
mod tests {
    use crate::{comments::comments, init::init_mssql};
    use std::{env, path::PathBuf, sync::Mutex};
    use tank::{
        AsValue, Connection, ConnectionPool, Driver, Entity, Executor, PoolConfig, Query,
        QueryResult, Row, expr, stream::TryStreamExt,
    };
    use tank_mssql::{MSSQLConnection, MSSQLDriver};
    use tank_tests::{execute_tests, init_logs, silent_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[derive(Entity, Debug, PartialEq)]
    #[tank(schema = "testing", name = "vector_items")]
    struct VectorItem {
        #[tank(primary_key)]
        id: i32,
        embedding: [f32; 3],
        payload: serde_json::Value,
    }

    async fn test_vector_and_json<D: Driver>(pool: &mut impl ConnectionPool<D>) {
        let mut conn = pool.get().await.expect("Could not get connection");
        VectorItem::drop_table(&mut conn, true, false)
            .await
            .expect("Failed to drop VectorItem table");
        VectorItem::create_table(&mut conn, true, true)
            .await
            .expect("Failed to create VectorItem table");
        let mut item = VectorItem {
            id: 1,
            embedding: [1.5, -2.25, 3.0],
            payload: serde_json::json!({"model": "v1", "dims": [1, 2, 3]}),
        };
        item.save(&mut conn)
            .await
            .expect("Failed to save VectorItem");
        let loaded = VectorItem::find_one(&mut conn, expr!(VectorItem::id == 1))
            .await
            .expect("Failed to query VectorItem")
            .expect("VectorItem not found");
        assert_eq!(loaded, item);

        item.embedding = [0.25, 0.5, -0.75];
        item.payload = serde_json::json!({"model": "v2", "active": true});
        item.save(&mut conn)
            .await
            .expect("Failed to update VectorItem");
        let loaded = VectorItem::find_one(&mut conn, expr!(VectorItem::id == 1))
            .await
            .expect("Failed to query updated VectorItem")
            .expect("Updated VectorItem not found");
        assert_eq!(loaded, item);
    }

    async fn test_results<D: Driver>(pool: &mut impl ConnectionPool<D>) {
        let mut conn = pool.get().await.expect("Could not get connection");
        conn.execute(
            "SET NOCOUNT OFF;
             DROP TABLE IF EXISTS #tank_results;
             CREATE TABLE #tank_results (id INT NOT NULL PRIMARY KEY);",
        )
        .await
        .expect("Failed to create result test table");

        for prepared in [false, true] {
            conn.execute(
                "TRUNCATE TABLE #tank_results;
                 INSERT INTO #tank_results (id) VALUES (1), (2), (3);",
            )
            .await
            .expect("Failed to seed result test table");

            for (sql, parameter, expected) in [
                (
                    "-- Leading comment\nUPDATE #tank_results SET id = id WHERE id <= @P1;",
                    0,
                    vec![(Some(0), None)],
                ),
                (
                    "/* Leading comment */ UPDATE #tank_results SET id = id WHERE id <= @P1;",
                    2,
                    vec![(Some(2), None)],
                ),
                (
                    "WITH targets AS (SELECT id FROM #tank_results WHERE id <= @P1)
                     UPDATE targets SET id = id;",
                    0,
                    vec![(Some(0), None)],
                ),
                (
                    "WITH targets AS (SELECT id FROM #tank_results WHERE id <= @P1)
                     UPDATE targets SET id = id;",
                    2,
                    vec![(Some(2), None)],
                ),
                (
                    "UPDATE #tank_results SET id = id + 10 WHERE id <= @P1;
                     SELECT id FROM #tank_results ORDER BY id;
                     SELECT id FROM #tank_results WHERE id = 0;
                     DELETE FROM #tank_results WHERE id > 10;",
                    2,
                    vec![
                        (Some(2), None),
                        (None, Some(3)),
                        (None, Some(11)),
                        (None, Some(12)),
                        (Some(2), None),
                    ],
                ),
                (
                    "INSERT INTO #tank_results (id) OUTPUT inserted.id VALUES (@P1);",
                    4,
                    vec![(None, Some(4))],
                ),
                (
                    "UPDATE #tank_results SET id = id + 1 OUTPUT inserted.id WHERE id = @P1;",
                    4,
                    vec![(None, Some(5))],
                ),
                (
                    "UPDATE #tank_results SET id = id + 1 OUTPUT inserted.id WHERE id = @P1;",
                    0,
                    vec![],
                ),
            ] {
                let query = if prepared {
                    let mut query = conn.prepare(sql).await.expect("Failed to prepare query");
                    query.bind(parameter).expect("Failed to bind parameter");
                    query
                } else {
                    Query::raw(sql.replace("@P1", &parameter.to_string()))
                };
                let results = conn
                    .run(query)
                    .map_ok(|result| match result {
                        QueryResult::Affected(affected) => (affected.rows_affected, None),
                        QueryResult::Row(row) => (
                            None,
                            Some(
                                i32::try_from_value(row.value(0).expect("Missing result id"))
                                    .expect("Failed to decode result id"),
                            ),
                        ),
                    })
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("Failed to run result query");
                assert_eq!(results, expected, "prepared={prepared}, sql={sql}");
            }
        }

        conn.execute("DROP TABLE #tank_results;")
            .await
            .expect("Failed to drop result test table");
    }

    #[tokio::test]
    pub async fn mssql() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = MSSQLDriver::new();

        // Unencrypted
        let (url, container) = init_mssql(false).await;
        let container = container.expect("Could not launch the container");
        let mut pool = driver
            .connect_pool(url, PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        test_vector_and_json(&mut pool).await;
        test_results(&mut pool).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mssql(true).await;
        let container = container.expect("Could not launch the SSL container");

        let url_parsed =
            Url::parse(&ssl_url).expect("Could not parse the url returned from init_mssql");
        let mut url_base = url_parsed.clone();
        url_base.set_query(None);
        let no_ca_url = url_base
            .query_pairs_mut()
            .extend_pairs(url_parsed.query_pairs().filter(|(k, _)| k != "ssl_ca"))
            .finish();
        silent_logs! {
            assert!(
                MSSQLConnection::connect(&driver, no_ca_url.to_string().into())
                    .await
                    .is_err(),
                "Connection should fail without ssl_ca when encrypt=true"
            );
        }

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        unsafe {
            env::set_var(
                "MSSQL_SSL_CA",
                path.join("tests/assets/ca.pem").to_str().unwrap(),
            );
        }
        MSSQLConnection::connect(&driver, no_ca_url.to_string().into())
            .await
            .expect("Connection should succeed with MSSQL_SSL_CA environment variable")
            .disconnect()
            .await
            .expect("Failed to disconnect");
        unsafe {
            env::remove_var("MSSQL_SSL_CA");
        }

        let mut pool = driver
            .connect_pool(ssl_url, PoolConfig::new())
            .await
            .expect("Failed to connect with SSL");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        test_vector_and_json(&mut pool).await;
        test_results(&mut pool).await;
        drop(container);
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        let driver = MSSQLDriver::new();
        silent_logs! {
            let pool = driver.connect_pool("postgres://some_url", PoolConfig::new()).await;
            assert!(pool.is_err() || pool.unwrap().get().await.is_err());
        }
    }
}
