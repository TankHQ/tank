mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_mysql;
    use std::sync::Mutex;
    use tank_core::{Connection, Driver, PoolConfig};
    use tank_mysql::{MySQLConnection, MySQLDriver};
    use tank_tests::{execute_tests, init_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn mysql() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = MySQLDriver::mysql();

        // Unencrypted
        let (url, container) = init_mysql(false).await;
        let container = container.expect("Could not launch the container");
        let mut pool = driver
            .connect_pool(url.clone().into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mysql(true).await;
        let container = container.expect("Could not launch the SSL container");

        let url = Url::parse(&url).expect("Could not parse the url returned from init");
        let mut url_base = url.clone();
        url_base.set_query(None);

        let no_cert_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_cert"))
            .finish();
        assert!(
            MySQLConnection::connect(&driver, no_cert_url.to_string().into())
                .await
                .is_err()
        );

        let no_pass_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_pass"))
            .finish();
        assert!(
            MySQLConnection::connect(&driver, no_pass_url.to_string().into())
                .await
                .is_err()
        );

        let mut pool = driver
            .connect_pool(ssl_url.to_string().into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }
}
