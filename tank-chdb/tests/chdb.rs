#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tank_chdb::ChDBDriver;
    use tank_core::{Driver, PoolConfig};
    use tank_tests::{execute_tests, init_logs};

    static MUTEX: Mutex<()> = Mutex::new(());

    unsafe extern "C" {
        fn chdb_set_signal_handlers_enabled(enabled: std::ffi::c_int);
    }

    #[tokio::test]
    pub async fn chdb() {
        init_logs();
        // chDB installs its own SIGSEGV/SIGABRT handlers that sleep for ~5 minutes
        // (signalHandler -> sleepForNanoseconds) before re-raising. That is why a
        // native crash shows up as "hangs forever, then SIGSEGV". Disable them so
        // the fault is immediate and produces a usable core / gdb backtrace.
        unsafe { chdb_set_signal_handlers_enabled(0) };
        std::panic::set_hook(Box::new(|info| {
            eprintln!("[tank] PANIC: {info}");
            let backtrace = std::backtrace::Backtrace::force_capture();
            eprintln!("[tank] PANIC BACKTRACE:\n{backtrace}");
        }));
        if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
            for line in maps.lines().filter(|l| l.contains("chdb")) {
                eprintln!("[tank] loaded lib: {line}");
            }
        }
        eprintln!(
            "[tank] cwd={:?} exe={:?}",
            std::env::current_dir(),
            std::env::current_exe()
        );
        std::thread::spawn(|| {
            let start = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                eprintln!(
                    "[tank] HEARTBEAT: test binary still alive after {:?} \
                     (if the last [tank] line above is a query/stream start, that is where it is stuck)",
                    start.elapsed()
                );
            }
        });
        let _guard = MUTEX.lock().unwrap();
        let driver = ChDBDriver::new();
        let mut pool = driver
            .connect_pool("chdb://".into(), PoolConfig::new())
            .await
            .expect("Could not open chDB");
        execute_tests(&mut pool).await;
    }
}
