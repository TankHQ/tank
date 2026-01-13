#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_tests::{execute_tests, init_logs};
    use tank_yourdb::YourDBDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn yourdb() {
        init_logs();
        const URL: &'static str = "yourdb://";
        let _lock = MUTEX.lock().unwrap();
        let driver = YourDBDriver::new();
        let connection = driver
            .connect(URL.into())
            .await
            .expect("Could not open the database");
        execute_tests(connection).await;
    }
}
