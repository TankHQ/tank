mod init;

#[cfg(test)]
mod tests {
    use super::init::{execute_tests, init};
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_tests::init_logs;
    use tank_valkey::ValkeyDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn valkey() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = ValkeyDriver::new();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{url}`");
        let connection = driver.connect(url.into()).await.expect(&error_msg);
        execute_tests(connection).await;
        drop(container);
    }
}
