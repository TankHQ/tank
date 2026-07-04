mod init;

#[cfg(test)]
mod tests {
    use super::init::{execute_tests, init_valkey};
    use std::sync::Mutex;
    use tank::{Driver, PoolConfig};
    use tank_tests::init_logs;
    use tank_valkey::ValkeyDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn valkey() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_valkey(false).await;
        let container = container.expect("Could not launch the container");
        {
            let driver: ValkeyDriver = ValkeyDriver::default();
            let mut pool = driver
                .connect_pool(url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool).await;
        }
        {
            let driver = ValkeyDriver::new(".", false);
            let mut pool = driver
                .connect_pool(url.clone().into(), PoolConfig::new())
                .await
                .expect("Failed to connect");
            execute_tests(&mut pool).await;
        }
        drop(container);

        // SSL
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        unsafe {
            std::env::set_var("SSL_CERT_FILE", path.join("tests/assets/ca.pem"));
        }
        let (url, container) = init_valkey(true).await;
        let container = container.expect("Could not launch the SSL container");
        let driver = ValkeyDriver::new(".", false);
        let mut pool = driver
            .connect_pool(url.clone().into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }
}
