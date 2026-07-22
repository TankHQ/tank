#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tank_chdb::ChdbDriver;
    use tank_core::{Driver, PoolConfig};
    use tank_tests::{execute_tests, init_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn chdb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = ChdbDriver::new();
        let mut pool = driver
            .connect_pool("chdb://".into(), PoolConfig::new())
            .await
            .expect("Could not open chDB");
        execute_tests(&mut pool).await;
    }
}
