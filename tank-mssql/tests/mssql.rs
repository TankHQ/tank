mod comments;
mod init;

#[cfg(test)]
mod tests {
    use crate::{comments::comments, init::init_mssql};
    use std::sync::Mutex;
    use tank::{Entity, expr};
    use tank_core::{ConnectionPool, Driver, PoolConfig};
    use tank_mssql::MSSQLDriver;
    use tank_tests::{execute_tests, init_logs, silent_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[derive(Entity, Debug, PartialEq)]
    #[tank(schema = "testing", name = "vector_items")]
    struct VectorItem {
        #[tank(primary_key)]
        id: i32,
        embedding: [f32; 3],
        payload: serde_json::Value,
    }

    #[tokio::test]
    pub async fn mssql() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = MSSQLDriver::new();

        let (url, container) = init_mssql().await;
        let mut pool = driver
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        {
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
        drop(container);
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        let driver = MSSQLDriver::new();
        silent_logs! {
            let pool = driver.connect_pool("postgres://some_url".into(), PoolConfig::new()).await;
            assert!(pool.is_err() || pool.unwrap().get().await.is_err());
        }
    }
}
