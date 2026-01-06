mod init;

#[cfg(test)]
mod tests {
    use crate::init::{execute_tests, init_scylladb};
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_scylladb::ScyllaDBDriver;
    use tank_tests::init_logs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn scylladb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_scylladb(false).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{url}`");
        let driver = ScyllaDBDriver::new();
        let connection = driver.connect(url.into()).await.expect(&error_msg);
        execute_tests(connection).await;
        drop(container);

        // SSL
        let (ssl_url, container) = init_scylladb(true).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{ssl_url}`");
        let driver = ScyllaDBDriver::new();
        let connection = driver.connect(ssl_url.into()).await.expect(&error_msg);
        execute_tests(connection).await;
        drop(container);
    }
}
