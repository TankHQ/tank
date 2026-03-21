mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_mariadb;
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_mysql::MariaDBDriver;
    use tank_tests::{execute_tests, init_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn mariadb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_mariadb(false).await;
        let container = container.expect("Could not launch container");
        let driver = MariaDBDriver::new();
        let connection = driver
            .connect(url.clone().into())
            .await
            .expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_mariadb(true).await;
        let container = container.expect("Could not launch container");
        let driver = MariaDBDriver::new();

        let url = Url::parse(&url).expect("Could not parse the url returned from init");
        let mut url_base = url.clone();
        url_base.set_query(None);

        let no_cert_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_cert"))
            .finish();
        assert!(
            driver
                .connect(no_cert_url.to_string().into())
                .await
                .is_err()
        );

        let no_pass_url = url_base
            .query_pairs_mut()
            .extend_pairs(url.clone().query_pairs().filter(|(k, _)| k != "ssl_pass"))
            .finish();
        assert!(
            driver
                .connect(no_pass_url.to_string().into())
                .await
                .is_err()
        );

        let connection = driver
            .connect(ssl_url.into())
            .await
            .expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);
    }
}
