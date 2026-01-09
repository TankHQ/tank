#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_tests::{execute_tests, init_logs};
    use tank_mongodb::MongoDBDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn mongodb() {
        init_logs();
        const URL: &'static str = "mongodb://";
        let _guard = MUTEX.lock().unwrap();
        let driver = MongoDBDriver::new();
        let connection = driver
            .connect(URL.into())
            .await
            .expect("Could not open the database");
        execute_tests(connection).await;
    }
}
