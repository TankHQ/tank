mod comments;
mod init;

#[cfg(test)]
mod tests {
    use crate::{comments::comments, init::init_mysql};
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
            .connect_pool(url.clone(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mysql(true).await;
        let container = container.expect("Could not launch the SSL container");

        let url = Url::parse(&ssl_url).expect("Could not parse the url returned from init");
        let mut no_cert_url = url.clone();
        no_cert_url.set_query(None);
        no_cert_url
            .query_pairs_mut()
            .extend_pairs(url.query_pairs().filter(|(k, _)| k != "ssl_cert"));
        assert!(
            MySQLConnection::connect(&driver, no_cert_url.to_string().into())
                .await
                .is_err()
        );

        let mut no_key_url = url.clone();
        no_key_url.set_query(None);
        no_key_url
            .query_pairs_mut()
            .extend_pairs(url.query_pairs().filter(|(k, _)| k != "ssl_key"));
        assert!(
            MySQLConnection::connect(&driver, no_key_url.to_string().into())
                .await
                .is_err()
        );

        let mut pool = driver
            .connect_pool(ssl_url, PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        drop(container);
    }
}
