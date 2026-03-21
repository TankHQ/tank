mod init;

#[cfg(test)]
mod tests {
    use crate::init::{execute_tests, init_cassandra};
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_scylladb::CassandraDriver;
    use tank_tests::init_logs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn cassandra() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init_cassandra(false).await;
        let container = container.expect("Could not launch container");
        let driver = CassandraDriver::new();
        let connection = driver.connect(url.into()).await.expect("Failed to connect");
        execute_tests(connection).await;
        drop(container);

        // SSL
        // let (ssl_url, container) = init_cassandra(true).await;
        // let container = container.expect("Could not launch container");
        // let driver = CassandraDriver::new();
        // let connection = driver.connect(ssl_url.into()).await.expect("Failed to connect");
        // execute_tests(connection).await;
        // drop(container);
    }
}
