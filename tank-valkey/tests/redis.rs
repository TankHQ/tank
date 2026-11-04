mod init;

#[cfg(test)]
mod tests {
    use super::init::{execute_tests, init_redis};
    use std::sync::Mutex;
    use tank_core::{Driver, PoolConfig};
    use tank_tests::init_logs;
    use tank_valkey::RedisDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn redis() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_redis(false).await;
        let container = container.expect("Could not launch the container");
        {
            let driver = RedisDriver::default();
            let mut pool = driver
                .connect_pool(url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool).await.expect("A tank-tests test failed");
        }
        {
            let driver = RedisDriver::new(".", false);
            let mut pool = driver
                .connect_pool(url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool).await.expect("A tank-tests test failed");
        }
        drop(container);

        // SSL
        let (ssl_url, container) = init_redis(true).await;
        let container = container.expect("Could not launch the SSL container");
        {
            let driver = RedisDriver::default();
            let mut pool = driver
                .connect_pool(ssl_url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool)
                .await
                .expect("A tank-tests test failed");
        }
        {
            let driver = RedisDriver::new(".", false);
            let mut pool = driver
                .connect_pool(ssl_url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool)
                .await
                .expect("A tank-tests test failed");
        }
        drop(container);
    }
}
