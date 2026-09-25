mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_clickhouse;
    use std::sync::Mutex;
    use tank_clickhouse::ClickHouseDriver;
    use tank_core::{Driver, PoolConfig};
    use tank_tests::{execute_tests, init_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn clickhouse() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        let (url, container) = init_clickhouse().await;
        let container = container.expect("Could not launch the ClickHouse container");
        let driver = ClickHouseDriver::new();
        let mut pool = driver
            .connect_pool(url.into(), PoolConfig::new())
            .await
            .expect("Failed to connect");
        execute_tests(&mut pool).await;
        drop(container);
    }
}
