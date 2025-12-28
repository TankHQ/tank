mod init;

#[cfg(test)]
mod tests {
    use crate::init::init_scylladb;
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_scylladb::ScyllaDBDriver;
    use tank_tests::{
        init_logs, interval, limits, simple, trade_multiple, trade_simple, transaction1,
    };

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
        let mut connection = driver.connect(url.clone().into()).await.expect(&error_msg);
        simple(&mut connection).await;
        trade_simple(&mut connection).await;
        trade_multiple(&mut connection).await;
        limits(&mut connection).await;
        interval(&mut connection).await;
        drop(container);
    }
}
