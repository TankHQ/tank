mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_scylladb;
    use std::sync::Mutex;
    use tank_core::{ConnectionPool, Driver, PoolConfig};
    use tank_scylladb::ScyllaDBDriver;
    use tank_tests::{init_logs, silent_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    async fn check_url_parameters(url: &str) {
        let driver = ScyllaDBDriver::new();
        let sep = if url.contains('?') { '&' } else { '?' };

        silent_logs! {
            {
                let url = format!("{url}{sep}hostname_resolution_timeout=12.5");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with valid float duration");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with valid float duration");
            }

            {
                let url = format!("{url}{sep}hostname_resolution_timeout=hello");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Pool creation should succeed before connection validation");
                assert!(
                    pool.get().await.is_err(),
                    "Should have failed to connect due to invalid duration parameter"
                );
            }

            {
                let url = format!("{url}{sep}tcp_nodelay=true");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with valid boolean parameter");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with valid boolean parameter");
            }

            {
                let url = format!("{url}{sep}local_ip_address=not_an_ip");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Pool creation should succeed before connection validation");
                assert!(
                    pool.get().await.is_err(),
                    "Should have failed due to invalid IP address"
                );
            }

            {
                let url = format!("{url}{sep}pool_size_per_host=4");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with valid usize parameter");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with valid usize parameter");
            }

            {
                let url = format!("{url}{sep}write_coalescing_delay=SmallNondeterministic");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with SmallNondeterministic delay");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with SmallNondeterministic delay");
            }

            {
                let url = format!("{url}{sep}write_coalescing_delay=50");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with numeric write_coalescing_delay");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with numeric write_coalescing_delay");
            }

            {
                let url = format!(
                    "{url}{sep}connection_timeout=10&tcp_keepalive_interval=15.5&compression=lz4"
                );
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Failed to connect with multiple valid parameters");
                let _connection = pool
                    .get()
                    .await
                    .expect("Could not get connection with multiple valid parameters");
            }

            {
                let url = format!("{url}{sep}this_parameter_does_not_exist=123");
                let pool = driver
                    .connect_pool(url.into(), PoolConfig::new())
                    .await
                    .expect("Pool creation should succeed before connection validation");
                assert!(
                    pool.get().await.is_err(),
                    "Should have failed due to unknown parameter"
                );
            }
        }
    }

    #[tokio::test]
    async fn url_parameters() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_scylladb(false).await;
        let container = container.expect("Could not launch container");
        check_url_parameters(&url).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_scylladb(true).await;
        let container = container.expect("Could not launch the SSL container");
        check_url_parameters(&ssl_url).await;
        drop(container);
    }
}
