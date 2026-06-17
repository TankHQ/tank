mod init;

#[cfg(test)]
mod tests {
    use super::init::{execute_tests, init_redis};
    use std::sync::Mutex;
    use tank_core::Connection;
    use tank_tests::init_logs;
    use tank_valkey::{RedisConnection, RedisDriver};

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
            let mut connection = RedisConnection::connect(&driver, url.clone().into())
                .await
                .expect("Failed to connect");
            execute_tests(&mut connection).await;
            connection.disconnect().await.expect("Failed to disconnect");
        }
        {
            let driver = RedisDriver::new(".", false);
            let mut connection = RedisConnection::connect(&driver, url.clone().into())
                .await
                .expect("Failed to connect");
            execute_tests(&mut connection).await;
            connection.disconnect().await.expect("Failed to disconnect");
        }
        drop(container);

        // SSL
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        unsafe {
            std::env::set_var("SSL_CERT_FILE", path.join("tests/assets/ca.pem"));
        }
        let (ssl_url, container) = init_redis(true).await;
        let container = container.expect("Could not launch the SSL container");
        let driver = RedisDriver::new(".", false);
        let mut connection = RedisConnection::connect(&driver, ssl_url.clone().into())
            .await
            .expect("Failed to connect");
        execute_tests(&mut connection).await;
        connection.disconnect().await.expect("Failed to disconnect");
        drop(container);
    }
}
