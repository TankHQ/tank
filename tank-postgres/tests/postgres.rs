mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::{env, path::PathBuf, sync::Mutex};
    use tank_core::{Connection, Driver};
    use tank_postgres::{PostgresConnection, PostgresDriver};
    use tank_tests::{execute_tests, init_logs, silent_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn postgres() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = PostgresDriver::new();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch the container");
        let connection = driver.connect(url.into()).await.expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);

        // SSL
        let (url, container) = init(true).await;
        let container = container.expect("Could not launch the SSL container");
        let connection = driver.connect(url.into()).await.expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            assert!(
                PostgresDriver::new().connect("mysql://some_url".into())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn check_tls() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url_full, container) = init(true).await;
        let url_full = Url::parse(&url_full).expect("Could not parse the url returned from init");
        let mut url_base = url_full.clone();
        url_base.set_query(None);
        let _container = container.expect("Could not launch container");
        let _ = PostgresConnection::connect(&Default::default(), url_full.to_string().into())
            .await
            .expect("Connection should succeed");
        let url = url_base
            .query_pairs_mut()
            .extend_pairs(url_full.query_pairs().filter(|(k, _)| k != "sslrootcert"))
            .finish();
        let _ = PostgresConnection::connect(&Default::default(), url.to_string().into())
            .await
            .expect_err("Connection should fail without sslrootcert");
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        unsafe {
            env::set_var(
                "PGSSLROOTCERT",
                path.join("tests/assets/root.crt").to_str().unwrap(),
            );
        }
        let connection = PostgresConnection::connect(&Default::default(), url.to_string().into())
            .await
            .expect("Connection should succeed with environment variable replacing sslrootcert");
        connection.disconnect().await.expect("Could not disconnect");
        unsafe {
            env::remove_var("PGSSLROOTCERT");
        }
        let _ = PostgresConnection::connect(&Default::default(), url.to_string().into())
            .await
            .expect_err("Connection should fail again without sslrootcert");
    }
}
