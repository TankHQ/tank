//! Mixed-workload benchmark measuring the driver and ORM overhead around DuckDB.
//!
//! Debug builds run a reduced workload as a smoke test; for meaningful numbers run:
//! `cargo test --release -p tank-duckdb --test benchmark -- --nocapture`
#[cfg(test)]
mod tests {
    use std::{
        cmp::min,
        fmt::{self, Display, Formatter},
        future::ready,
        time::{Duration, Instant},
    };
    use tank::{
        AsValue, Connection, Entity, Executor, QueryBuilder, Row, Transaction, cols, expr,
        stream::TryStreamExt,
    };
    use tank_duckdb::{DuckDBConnection, DuckDBDriver};
    use tank_tests::init_logs;
    use time::{Duration as TimeDuration, PrimitiveDateTime, macros::datetime};

    const SCALE: usize = if cfg!(debug_assertions) { 1 } else { 10 };
    const ROWS: usize = 10_000 * SCALE;
    const ROUNDS: usize = if cfg!(debug_assertions) { 1 } else { 7 };
    const LOOKUPS: usize = 500 * SCALE;
    const RANGES: usize = 20 * SCALE;
    const FILTERS: usize = 10 * SCALE;
    const AGGREGATES: usize = 5 * SCALE;
    const SINGLE_WRITES: usize = 50 * SCALE;
    const MIXED: usize = 200 * SCALE;
    const ACCOUNTS: usize = 512;
    const SYMBOLS: [&str; 16] = [
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK.B", "JPM", "V",
        "XOM", "UNH", "MA", "COST", "HD",
    ];

    #[derive(Entity, Debug, Clone, PartialEq)]
    #[tank(name = "bench_trades")]
    struct Trade {
        #[tank(primary_key)]
        id: i64,
        account: i32,
        symbol: String,
        buy: bool,
        quantity: i32,
        price: f64,
        note: String,
        memo: Option<String>,
        executed_at: PrimitiveDateTime,
    }

    impl Trade {
        fn generate(id: usize) -> Self {
            Self {
                id: id as i64,
                account: (id % ACCOUNTS) as i32,
                symbol: SYMBOLS[id % SYMBOLS.len()].into(),
                buy: id % 3 != 0,
                quantity: (id * 37 % 1000 + 1) as i32,
                price: 50.0 + (id * 7919 % 100_000) as f64 / 100.0,
                note: format!("order {id:08} routed via smart order router"),
                memo: (id % 4 == 0).then(|| format!("client memo #{id}")),
                executed_at: datetime!(2026-01-02 09:30:00) + TimeDuration::milliseconds(id as i64),
            }
        }
    }

    /// Fastest observed time per phase across rounds, with the work each round performs.
    #[derive(Default)]
    struct Report(Vec<(&'static str, usize, Duration)>);

    impl Report {
        fn record(&mut self, name: &'static str, ops: usize, elapsed: Duration) {
            match self.0.iter_mut().find(|(n, ..)| *n == name) {
                Some((_, _, best)) => *best = min(*best, elapsed),
                None => self.0.push((name, ops, elapsed)),
            }
        }
    }

    impl Display for Report {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            writeln!(
                f,
                "DuckDB benchmark: {ROWS} rows, best of {ROUNDS} rounds\n{:<20} {:>8} {:>12} {:>12}",
                "phase", "ops", "total", "per op"
            )?;
            let mut total = Duration::ZERO;
            for &(name, ops, elapsed) in &self.0 {
                total += elapsed;
                writeln!(
                    f,
                    "{name:<20} {ops:>8} {:>12} {:>12}",
                    format!("{elapsed:.2?}"),
                    format!("{:.2?}", elapsed / ops as u32),
                )?;
            }
            write!(
                f,
                "{:<20} {:>8} {:>12}",
                "total",
                "",
                format!("{total:.2?}")
            )
        }
    }

    /// Deterministic, well spread sequence of ids in `0..ROWS`.
    fn spread(i: usize) -> i64 {
        (i * 7919 % ROWS) as i64
    }

    async fn round(connection: &mut DuckDBConnection, trades: &[Trade], report: &mut Report) {
        Trade::drop_table(connection, true, false).await.unwrap();
        Trade::create_table(connection, true, false).await.unwrap();

        // Bulk load through the appender.
        let start = Instant::now();
        let affected = Trade::insert_many(connection, trades).await.unwrap();
        report.record("append", ROWS, start.elapsed());
        assert_eq!(affected.rows_affected, Some(ROWS as u64));

        // Full table scan decoded into entities.
        let start = Instant::now();
        let loaded: Vec<Trade> = Trade::find_many(connection, true, None)
            .try_collect()
            .await
            .unwrap();
        report.record("scan", ROWS, start.elapsed());
        assert_eq!(loaded.len(), ROWS);
        assert_eq!(loaded[ROWS / 2], trades[ROWS / 2]);

        // Ad hoc filtered queries, rendered and parsed each time.
        let start = Instant::now();
        let mut found = 0;
        for i in 0..FILTERS {
            let account = (i * 13 % ACCOUNTS) as i32;
            found += Trade::find_many(
                connection,
                expr!(Trade::account == #account && Trade::buy == true),
                None,
            )
            .try_fold(0, |n, _| ready(Ok(n + 1)))
            .await
            .unwrap();
        }
        report.record("filter", FILTERS, start.elapsed());
        assert!(found > 0);

        // Prepared point lookups by primary key.
        let mut by_id = Trade::prepare_find(connection, expr!(Trade::id == ?), Some(1))
            .await
            .unwrap();
        let start = Instant::now();
        for i in 0..LOOKUPS {
            let id = spread(i);
            by_id.bind(id).unwrap();
            let trade: Vec<Trade> = connection
                .fetch(&mut by_id)
                .and_then(|row| ready(Trade::from_row(row)))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(trade[0].id, id);
        }
        report.record("lookup_prepared", LOOKUPS, start.elapsed());

        // Prepared range queries binding a string and an integer.
        let mut by_symbol = Trade::prepare_find(
            connection,
            expr!(Trade::symbol == ? && Trade::account < ?),
            None,
        )
        .await
        .unwrap();
        let start = Instant::now();
        for i in 0..RANGES {
            let symbol = SYMBOLS[i % SYMBOLS.len()];
            by_symbol.bind(symbol).unwrap().bind(64).unwrap();
            let trades: Vec<Trade> = connection
                .fetch(&mut by_symbol)
                .and_then(|row| ready(Trade::from_row(row)))
                .try_collect()
                .await
                .unwrap();
            assert!(trades.iter().all(|t| t.symbol == symbol && t.account < 64));
        }
        report.record("range_prepared", RANGES, start.elapsed());

        // Aggregation decoded through the raw row interface.
        let query = QueryBuilder::new()
            .select(cols!(
                Trade::symbol,
                COUNT(*),
                SUM(Trade::quantity),
                AVG(Trade::price)
            ))
            .from(Trade::table())
            .where_expr(expr!(Trade::buy == true))
            .group_by(cols!(Trade::symbol))
            .build(&connection.driver());
        let sql = query.as_str().into_owned();
        let start = Instant::now();
        for _ in 0..AGGREGATES {
            let groups = connection
                .fetch(sql.as_str())
                .try_fold(0, |groups, row| {
                    ready(
                        String::try_from_value(row.value(0).unwrap())
                            .and_then(|_| i64::try_from_value(row.value(1).unwrap()))
                            .and_then(|_| f64::try_from_value(row.value(3).unwrap()))
                            .map(|_| groups + 1),
                    )
                })
                .await
                .unwrap();
            assert_eq!(groups, SYMBOLS.len());
        }
        report.record("aggregate", AGGREGATES, start.elapsed());

        // Single-row writes going through the SQL writer.
        let start = Instant::now();
        for i in 0..SINGLE_WRITES {
            Trade::insert_one(connection, &Trade::generate(ROWS + i))
                .await
                .unwrap();
        }
        report.record("insert_one", SINGLE_WRITES, start.elapsed());

        let start = Instant::now();
        for i in 0..SINGLE_WRITES {
            let mut trade = loaded[spread(i) as usize].clone();
            trade.quantity += 1;
            trade.memo = Some("amended".into());
            trade.save(connection).await.unwrap();
        }
        report.record("save", SINGLE_WRITES, start.elapsed());

        // Interleaved workload resembling an application serving requests.
        let start = Instant::now();
        let mut next_id = ROWS + SINGLE_WRITES;
        for i in 0..MIXED {
            by_id.bind(spread(i)).unwrap();
            let trade = connection
                .fetch(&mut by_id)
                .and_then(|row| ready(Trade::from_row(row)))
                .try_next()
                .await
                .unwrap()
                .unwrap();
            match i % 10 {
                0 => {
                    let batch = (next_id..next_id + 32)
                        .map(Trade::generate)
                        .collect::<Vec<_>>();
                    next_id += batch.len();
                    Trade::insert_many(connection, &batch).await.unwrap();
                }
                3 => {
                    let mut trade = trade;
                    trade.price *= 1.01;
                    trade.save(connection).await.unwrap();
                }
                7 => {
                    let account = trade.account;
                    Trade::find_many(connection, expr!(Trade::account == #account), Some(16))
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
        report.record("mixed", MIXED, start.elapsed());

        // Bulk maintenance inside a transaction.
        let start = Instant::now();
        let mut tx = connection.begin().await.unwrap();
        let updated = tx
            .execute("UPDATE bench_trades SET price = price * 1.001 WHERE buy = true")
            .await
            .unwrap();
        let deleted = Trade::delete_many(&mut tx, expr!(Trade::account >= 500))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        report.record("transaction", 1, start.elapsed());
        assert!(updated.rows_affected.unwrap() > 0 && deleted.rows_affected.unwrap() > 0);
    }

    #[tokio::test]
    async fn duckdb_benchmark() {
        init_logs();
        // A single DuckDB thread keeps timings stable, isolating the overhead outside the engine.
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        let trades = (0..ROWS).map(Trade::generate).collect::<Vec<_>>();
        let mut report = Report::default();
        for _ in 0..ROUNDS {
            round(&mut connection, &trades, &mut report).await;
        }
        println!("{report}");
    }
}
