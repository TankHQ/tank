mod comments;
mod init;

#[cfg(test)]
mod tests {
    use super::{comments::comments, init::init};
    use std::{env, path::PathBuf, sync::Mutex};
    use tank::{Connection, ConnectionPool, Driver, Executor, PoolConfig};
    use tank_postgres::{PostgresConnection, PostgresDriver};
    use tank_tests::{execute_tests, init_logs, silent_logs};
    use url::Url;

    static MUTEX: Mutex<()> = Mutex::new(());
    static DRIVER: PostgresDriver = PostgresDriver::new();

    #[tokio::test]
    pub async fn postgres() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch the container");
        let mut pool = DRIVER
            .connect_pool(url, PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        comments(&mut pool).await;
        drop(container);

        // SSL
        let (url, container) = init(true).await;
        let container = container.expect("Could not launch the SSL container");
        let mut pool = DRIVER
            .connect_pool(url, PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }

    #[tokio::test]
    async fn wrong_url() {
        init_logs();
        silent_logs! {
            let pool = DRIVER.connect_pool("mysql://some_url", PoolConfig::new()).await;
            assert!(pool.is_err() || pool.unwrap().get().await.is_err());
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
        let mut connection = PostgresConnection::connect(&DRIVER, url_full.to_string().into())
            .await
            .expect("Connection should succeed");
        connection.execute("SELECT 1").await.unwrap();
        let mut dns_url = url_full.clone();
        dns_url.set_host(Some("localhost")).unwrap();
        PostgresConnection::connect(&DRIVER, dns_url.to_string().into())
            .await
            .expect("Connection should verify a DNS hostname as well as an IP address");
        let url = url_base
            .query_pairs_mut()
            .extend_pairs(url_full.query_pairs().filter(|(k, _)| k != "sslrootcert"))
            .finish();
        let _ = PostgresConnection::connect(&PostgresDriver::new(), url.to_string().into())
            .await
            .expect_err("Connection should fail without sslrootcert");
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let variables = [
            ("PGSSLMODE", "verify-full".into()),
            (
                "PGSSLROOTCERT",
                path.join("tests/assets/root.crt").into_os_string(),
            ),
            (
                "PGSSLCERT",
                path.join("tests/assets/client.crt").into_os_string(),
            ),
            (
                "PGSSLKEY",
                path.join("tests/assets/client.key").into_os_string(),
            ),
        ];
        let previous = variables.map(|(key, value)| {
            let previous = env::var_os(key);
            // All connection tests in this process serialize environment access with MUTEX.
            unsafe { env::set_var(key, value) };
            (key, previous)
        });
        let mut env_url = url_full.clone();
        env_url.set_query(None);
        let result = PostgresConnection::connect(&DRIVER, env_url.to_string().into()).await;
        for (key, value) in previous {
            unsafe {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
        result
            .expect("Connection should succeed with TLS options from the environment")
            .execute("SELECT 1")
            .await
            .unwrap();
        let _ = PostgresConnection::connect(&PostgresDriver::new(), url.to_string().into())
            .await
            .expect_err("Connection should fail again without sslrootcert");
    }
}
