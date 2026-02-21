mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_mongodb::{MongoDBDriver, like_to_regex};
    use tank_tests::{execute_tests, init_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn mongodb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = MongoDBDriver::new();

        // Unencrypted
        let (url, container) = init(false).await;
        let container = container.expect("Could not launch container");
        let error_msg = format!("Could not connect to `{url}`");
        let connection = driver.connect(url.into()).await.expect(&error_msg);
        execute_tests(connection).await;
        drop(container);
    }

    #[test]
    fn regex_transform() {
        assert_eq!(like_to_regex("_"), r"^.$");
        assert_eq!(like_to_regex("%"), r"^.*$");
        assert_eq!(like_to_regex("AB%"), "^AB.*$");
        assert_eq!(like_to_regex("A%B"), r"^A.*B$");
        assert_eq!(like_to_regex("X_Y"), r"^X.Y$");
        assert_eq!(like_to_regex(r"a\b\c"), r"^a\\b\\c$");
        assert_eq!(like_to_regex("%[test]%"), r"^.*\[test\].*$");
    }
}
