mod comments;
mod init;

#[cfg(test)]
mod tests {
    use crate::{comments::comments, init::init_mariadb};
    use std::sync::Mutex;
    use tank_core::{Connection, Driver, PoolConfig};
    use tank_mysql::{MariaDBConnection, MariaDBDriver};
    use tank_tests::{execute_tests, init_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn mariadb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_mariadb(false).await;
        let container = container.expect("Could not launch the container");
        let driver = MariaDBDriver::mariadb();
        let mut pool = driver
            .connect_pool(url.clone().into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mariadb(true).await;
        let container = container.expect("Could not launch the SSL container");
        let driver = MariaDBDriver::mariadb();

        let url = Url::parse(&ssl_url).expect("Could not parse the url returned from init");
        let mut no_cert_url = url.clone();
        no_cert_url.set_query(None);
        no_cert_url
            .query_pairs_mut()
            .extend_pairs(url.query_pairs().filter(|(k, _)| k != "ssl_cert"));
        assert!(
            MariaDBConnection::connect(&driver, no_cert_url.to_string().into())
                .await
                .is_err()
        );

        let mut no_key_url = url.clone();
        no_key_url.set_query(None);
        no_key_url
            .query_pairs_mut()
            .extend_pairs(url.query_pairs().filter(|(k, _)| k != "ssl_key"));
        assert!(
            MariaDBConnection::connect(&driver, no_key_url.to_string().into())
                .await
                .is_err()
        );

        let mut pool = driver
            .connect_pool(ssl_url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        drop(container);
    }
}
