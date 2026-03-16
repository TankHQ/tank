mod init;

#[cfg(test)]
mod tests {
    use super::init::{execute_tests, init};
    use std::sync::Mutex;
    use tank_core::{Connection, Driver};
    use tank_tests::init_logs;
    use tank_valkey::ValkeyDriver;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn valkey() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch container");
        {
            let driver = ValkeyDriver::default();
            let error_msg = format!("Could not connect to `{url}`");
            let mut connection = driver.connect(url.clone().into()).await.expect(&error_msg);
            execute_tests(&mut connection).await;
            connection.disconnect().await.expect("Failed to disconnect");
        }
        {
            let driver = ValkeyDriver::new(".", false);
            let error_msg = format!("Could not connect to `{url}`");
            let mut connection = driver.connect(url.clone().into()).await.expect(&error_msg);
            execute_tests(&mut connection).await;
            connection.disconnect().await.expect("Failed to disconnect");
        }
        drop(container);
    }
}
