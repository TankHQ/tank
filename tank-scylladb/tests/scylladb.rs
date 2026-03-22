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
    pub async fn scylladb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_scylladb(false).await;
        let container = container.expect("Could not launch the container");
        let driver = ScyllaDBDriver::new();
        let connection = driver.connect(url.into()).await.expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);

        // SSL
        let (url, container) = init_scylladb(true).await;
        let container = container.expect("Could not launch the SSL container");
        let driver = ScyllaDBDriver::new();
        let connection = driver.connect(url.into()).await.expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);
    }
}
