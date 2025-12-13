mod init;

#[cfg(test)]
mod tests {
    use crate::init::init;
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_mysql::MySQLDriver;
    use tank_tests::{execute_tests, init_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn mysql() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{url}`");
        let driver = MySQLDriver::new();
        let connection = driver.connect(url.clone().into()).await.expect(&error_msg);
        execute_tests(connection).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init(true).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{ssl_url}`");
        let driver = MySQLDriver::new();

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
            .connect(ssl_url.to_string().into())
            .await
            .expect(&error_msg);
        execute_tests(connection).await;
        drop(container);
    }
}
