//! Mixed-workload benchmark measuring the driver and ORM overhead around SQLite.
//!
//! Debug builds run a reduced workload as a smoke test. For meaningful numbers run:
//! `cargo test --release -p tank-sqlite --test benchmark -- --nocapture`
#[cfg(test)]
mod tests {
    use tank::{AsValue, Connection, Executor, Row, stream::TryStreamExt};
    use tank_sqlite::{SQLiteConnection, SQLiteDriver};
    use tank_tests::{BenchmarkConfig, CountingAllocator, benchmark, init_logs};

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    #[tokio::test]
    async fn sqlite_benchmark() {
        init_logs();
        let mut connection =
            SQLiteConnection::connect(&SQLiteDriver::new(), "sqlite://:memory:".into())
                .await
                .unwrap();
        let config = BenchmarkConfig::default();
        benchmark(&mut connection, &config).await;

        // Outside timed phases: expose engine work that driver optimizations cannot remove.
        for (phase, sql) in [
            (
                "filter",
                "SELECT * FROM bench_trades WHERE account = 13 AND buy = 1",
            ),
            (
                "range_prepared",
                "SELECT * FROM bench_trades WHERE symbol = 'AAPL' AND account < 64",
            ),
            (
                "aggregate",
                "SELECT symbol, COUNT(*), SUM(quantity), AVG(price) FROM bench_trades WHERE buy = 1 GROUP BY symbol",
            ),
        ] {
            let plan = connection
                .fetch(format!("EXPLAIN QUERY PLAN {sql}"))
                .map_ok(|row| String::decode_value(row.value(3).unwrap()).unwrap())
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            println!("{phase}: {}", plan.join("; "));
        }
    }
}
