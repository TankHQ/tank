mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_mariadb;
    use std::sync::Mutex;
    use tank_core::{Connection, Driver};
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
        let driver = MariaDBDriver::new();
        let mut pool = driver
            .connect_pool(url.clone().into())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mariadb(true).await;
        let container = container.expect("Could not launch the SSL container");
        let driver = MariaDBDriver::new();

        let url = Url::parse(&url).expect("Could not parse the url returned from init");
        let mut url_base = url.clone();
        url_base.set_query(None);

        let no_cert_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_cert"))
            .finish();
        assert!(
            MariaDBConnection::connect(&driver, no_cert_url.to_string().into())
                .await
                .is_err()
        );

        let no_pass_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_pass"))
            .finish();
        assert!(
            MariaDBConnection::connect(&driver, no_pass_url.to_string().into())
                .await
                .is_err()
        );

        let mut pool = driver
            .connect_pool(ssl_url.into())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }
}
