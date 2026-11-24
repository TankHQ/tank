#[cfg(test)]
mod tests {
    use chdb_rust::{
        connection::Connection as RawConnection,
        format::{InputFormat, OutputFormat},
    };
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cmp::min,
        fmt::{self, Display, Formatter},
        future::ready,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    use tank::{AsValue, Connection, Entity, Executor, Row, Sql, cols, expr, stream::TryStreamExt};
    use tank_chdb::{ChDBConnection, ChDBDriver};
    use tank_tests::init_logs;
    use time::{Duration as TimeDuration, PrimitiveDateTime, macros::datetime};

    /// Counts Rust heap allocations and bytes, not chDB's internal C++ allocator.
    struct CountingAllocator;

    static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
    static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size, Ordering::Relaxed);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    const SCALE: usize = if cfg!(debug_assertions) { 1 } else { 10 };
    const ROWS: usize = 10_000 * SCALE;
    const ROUNDS: usize = if cfg!(debug_assertions) { 1 } else { 5 };
    const LOOKUPS: usize = 50 * SCALE;
    const RANGES: usize = 20 * SCALE;
    const FILTERS: usize = 10 * SCALE;
    const AGGREGATES: usize = 5 * SCALE;
    const SINGLE_WRITES: usize = 20 * SCALE;
    const MIXED: usize = 50 * SCALE;
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

    struct Measurement {
        allocations: usize,
        bytes: usize,
        start: Instant,
    }

    impl Measurement {
        fn start() -> Self {
            Self {
                allocations: ALLOCATIONS.load(Ordering::Relaxed),
                bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
                start: Instant::now(),
            }
        }

        fn finish(self) -> (Duration, usize, usize) {
            (
                self.start.elapsed(),
                ALLOCATIONS.load(Ordering::Relaxed) - self.allocations,
                ALLOCATED_BYTES.load(Ordering::Relaxed) - self.bytes,
            )
        }
    }

    /// Fastest observed time, allocation count, and bytes per phase across rounds.
    #[derive(Default)]
    struct Report(Vec<(&'static str, usize, Duration, usize, usize)>);

    impl Report {
        fn record(&mut self, name: &'static str, ops: usize, sample: (Duration, usize, usize)) {
            let (elapsed, allocations, bytes) = sample;
            match self.0.iter_mut().find(|(n, ..)| *n == name) {
                Some((_, _, best, best_allocs, best_bytes)) => {
                    *best = min(*best, elapsed);
                    *best_allocs = min(*best_allocs, allocations);
                    *best_bytes = min(*best_bytes, bytes);
                }
                None => self.0.push((name, ops, elapsed, allocations, bytes)),
            }
        }
    }

    impl Display for Report {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            writeln!(
                f,
                "chDB benchmark: {ROWS} rows, best of {ROUNDS} rounds\n\
                 {:<20} {:>8} {:>12} {:>12} {:>10} {:>12}",
                "phase", "ops", "total", "per op", "allocs/op", "bytes/op"
            )?;
            let mut total = Duration::ZERO;
            for &(name, ops, elapsed, allocations, bytes) in &self.0 {
                total += elapsed;
                writeln!(
                    f,
                    "{name:<20} {ops:>8} {:>12} {:>12} {:>10.1} {:>12.0}",
                    format!("{elapsed:.2?}"),
                    format!("{:.2?}", elapsed / ops as u32),
                    allocations as f64 / ops as f64,
                    bytes as f64 / ops as f64,
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

    async fn round(connection: &mut ChDBConnection, trades: &[Trade], report: &mut Report) {
        Trade::drop_table(connection, true, false).await.unwrap();
        Trade::create_table(connection, true, false).await.unwrap();

        // Bulk load through `append`.
        let m = Measurement::start();
        Trade::insert_many(connection, trades).await.unwrap();
        report.record("append", ROWS, m.finish());

        // Full table scan decoded into entities.
        let m = Measurement::start();
        let loaded: Vec<Trade> = Trade::find_many(connection, true, None)
            .try_collect()
            .await
            .unwrap();
        report.record("scan", ROWS, m.finish());
        assert_eq!(loaded.len(), ROWS);
        let mid = (ROWS / 2) as i64;
        assert_eq!(loaded.iter().find(|t| t.id == mid), Some(&trades[ROWS / 2]));

        // Ad hoc filtered queries, rendered and parsed each time.
        let m = Measurement::start();
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
        report.record("filter", FILTERS, m.finish());
        assert!(found > 0);

        // Prepared point lookups by primary key.
        let mut by_id = Trade::prepare_find(connection, expr!(Trade::id == ?), Some(1))
            .await
            .unwrap();
        let m = Measurement::start();
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
        report.record("lookup_prepared", LOOKUPS, m.finish());

        // Prepared range queries binding a string and an integer.
        let mut by_symbol = Trade::prepare_find(
            connection,
            expr!(Trade::symbol == ? && Trade::account < ?),
            None,
        )
        .await
        .unwrap();
        let m = Measurement::start();
        for i in 0..RANGES {
            let symbol = SYMBOLS[i % SYMBOLS.len()];
            by_symbol.bind(symbol).unwrap().bind(64).unwrap();
            let found_trades: Vec<Trade> = connection
                .fetch(&mut by_symbol)
                .and_then(|row| ready(Trade::from_row(row)))
                .try_collect()
                .await
                .unwrap();
            assert!(
                found_trades
                    .iter()
                    .all(|t| t.symbol == symbol && t.account < 64)
            );
        }
        report.record("range_prepared", RANGES, m.finish());

        // Aggregation decoded through the raw row interface.
        let query = Sql()
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
        let m = Measurement::start();
        for _ in 0..AGGREGATES {
            let groups = connection
                .fetch(sql.as_str())
                .try_fold(0, |groups, row| {
                    ready(
                        String::decode_value(row.value(0).unwrap())
                            .and_then(|_| i64::decode_value(row.value(1).unwrap()))
                            .and_then(|_| f64::decode_value(row.value(3).unwrap()))
                            .map(|_| groups + 1),
                    )
                })
                .await
                .unwrap();
            assert_eq!(groups, SYMBOLS.len());
        }
        report.record("aggregate", AGGREGATES, m.finish());

        // Single-row writes going through the SQL writer.
        let m = Measurement::start();
        for i in 0..SINGLE_WRITES {
            Trade::insert_one(connection, &Trade::generate(ROWS + i))
                .await
                .unwrap();
        }
        report.record("insert_one", SINGLE_WRITES, m.finish());

        let m = Measurement::start();
        for i in 0..SINGLE_WRITES {
            let mut trade = trades[spread(i) as usize].clone();
            trade.quantity += 1;
            trade.memo = Some("amended".into());
            trade.save(connection).await.unwrap();
        }
        report.record("save", SINGLE_WRITES, m.finish());

        // Interleaved workload resembling an application serving requests.
        let m = Measurement::start();
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
        report.record("mixed", MIXED, m.finish());

        // Bulk maintenance (chDB does not support transactions).
        let m = Measurement::start();
        connection
            .execute(
                Sql()
                    .update(Trade::table())
                    .set(cols!(Trade::price == Trade::price * 1.001))
                    .where_expr(expr!(Trade::buy == true))
                    .build(&connection.driver()),
            )
            .await
            .unwrap();
        Trade::delete_many(connection, expr!(Trade::account >= 500))
            .await
            .unwrap();
        report.record("maintenance", 1, m.finish());
    }

    #[tokio::test]
    async fn chdb_benchmark() {
        init_logs();
        let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
            .await
            .unwrap();
        let trades = (0..ROWS).map(Trade::generate).collect::<Vec<_>>();
        let mut report = Report::default();
        for _ in 0..ROUNDS {
            round(&mut connection, &trades, &mut report).await;
        }

        // Run separately so this diagnostic does not warm caches for the original phases.
        Trade::drop_table(&mut connection, true, false)
            .await
            .unwrap();
        Trade::create_table(&mut connection, true, false)
            .await
            .unwrap();
        Trade::insert_many(&mut connection, &trades).await.unwrap();
        for _ in 0..ROUNDS {
            let m = Measurement::start();
            let sum = connection
                .fetch("SELECT * FROM bench_trades")
                .try_fold(0i64, |sum, row| {
                    ready(i64::decode_value(row.value(0).unwrap()).map(|id| sum + id))
                })
                .await
                .unwrap();
            let sample = m.finish();
            report.record("raw_stream", ROWS, sample);
            assert_eq!(sum, (ROWS * (ROWS - 1) / 2) as i64);
            assert!(
                sample.1 < ROWS / 100,
                "{} allocations for {ROWS} rows",
                sample.1
            );
        }
        println!("{report}");

        // Outside timed phases: isolate engine execution, format serialization, and
        // streaming insert cost from Tank's adapter overhead on the same table.
        let mut raw = RawConnection::open_in_memory().unwrap();
        raw.query("SET final=1", OutputFormat::Null).unwrap();
        println!("\nRaw chDB scan baseline ({ROWS} rows, final=1, best of {ROUNDS} rounds):");
        for (label, format) in [
            ("engine only (Null)", OutputFormat::Null),
            ("JSONCompact", OutputFormat::JSONCompact),
            ("RowBinary", OutputFormat::RowBinary),
            ("Native", OutputFormat::Native),
            ("ArrowStream", OutputFormat::ArrowStream),
        ] {
            let mut best = Duration::MAX;
            let mut payload_bytes = 0;
            for _ in 0..ROUNDS {
                let start = Instant::now();
                let res = raw.query("SELECT * FROM bench_trades", format).unwrap();
                let elapsed = start.elapsed();
                payload_bytes = res.data_ref().len();
                best = min(best, elapsed);
            }
            println!(
                "  query  {label:<20} {:>10} ({:>10}/row, {:>8.1} KiB)",
                format!("{best:.2?}"),
                format!("{:.2?}", best / ROWS as u32),
                payload_bytes as f64 / 1024.0,
            );
        }
        for (label, format) in [
            ("JSONCompact", OutputFormat::JSONCompact),
            ("RowBinary", OutputFormat::RowBinary),
            ("Native", OutputFormat::Native),
        ] {
            let mut best = Duration::MAX;
            let mut chunks = 0usize;
            for _ in 0..ROUNDS {
                let start = Instant::now();
                let mut stream = raw
                    .query_stream("SELECT * FROM bench_trades", format)
                    .unwrap();
                let mut c = 0;
                while let Some(_chunk) = stream.next_chunk().unwrap() {
                    c += 1;
                }
                let elapsed = start.elapsed();
                chunks = c;
                best = min(best, elapsed);
            }
            println!(
                "  stream {label:<20} {:>10} ({:>10}/row, {chunks} chunks)",
                format!("{best:.2?}"),
                format!("{:.2?}", best / ROWS as u32),
            );
        }

        // Compare direct RowBinary insert_stream vs Tank's SQL VALUES append.
        let mut best_insert_stream = Duration::MAX;
        let mut best_insert_allocs = usize::MAX;
        let mut best_insert_bytes = usize::MAX;
        for _ in 0..ROUNDS {
            raw.query("TRUNCATE TABLE bench_trades", OutputFormat::Null)
                .unwrap();
            let m = Measurement::start();
            let mut ins = raw
                .insert_stream(
                    "INSERT INTO bench_trades (id, account, symbol, buy, quantity, price, note, memo, executed_at)",
                    InputFormat::RowBinary,
                )
                .unwrap();
            let mut buf = Vec::with_capacity(64 * 1024);
            for t in &trades {
                buf.extend_from_slice(&t.id.to_le_bytes());
                buf.extend_from_slice(&t.account.to_le_bytes());
                write_rb_str(&mut buf, &t.symbol);
                buf.push(u8::from(t.buy));
                buf.extend_from_slice(&t.quantity.to_le_bytes());
                buf.extend_from_slice(&t.price.to_le_bytes());
                write_rb_str(&mut buf, &t.note);
                match &t.memo {
                    Some(m) => {
                        buf.push(0);
                        write_rb_str(&mut buf, m);
                    }
                    None => buf.push(1),
                }
                let nanos = t.executed_at.assume_utc().unix_timestamp_nanos() as i64;
                buf.extend_from_slice(&nanos.to_le_bytes());
                if buf.len() >= 64 * 1024 {
                    ins.append(&buf).unwrap();
                    buf.clear();
                }
            }
            if !buf.is_empty() {
                ins.append(&buf).unwrap();
            }
            let stats = ins.finish().unwrap();
            let (elapsed, allocs, bytes) = m.finish();
            assert_eq!(stats.rows_written, ROWS as u64);
            best_insert_stream = min(best_insert_stream, elapsed);
            best_insert_allocs = min(best_insert_allocs, allocs);
            best_insert_bytes = min(best_insert_bytes, bytes);
        }
        println!(
            "\nRaw chDB insert_stream (RowBinary, {ROWS} rows): {:.2?} ({:.2?}/row, {:.2} allocs/row, {:.0} bytes/row)",
            best_insert_stream,
            best_insert_stream / ROWS as u32,
            best_insert_allocs as f64 / ROWS as f64,
            best_insert_bytes as f64 / ROWS as f64,
        );

        // Compare direct point lookup with native params vs Tank's SET + stream + JSON path.
        let mut best_lookup_raw = Duration::MAX;
        for _ in 0..ROUNDS {
            let start = Instant::now();
            for i in 0..LOOKUPS {
                let id = spread(i);
                let res = raw
                    .query_with_params(
                        "SELECT * FROM bench_trades WHERE id = {p0:Int64} LIMIT 1",
                        OutputFormat::RowBinary,
                        [("p0", id)],
                    )
                    .unwrap();
                assert_eq!(res.rows_read(), 1);
            }
            best_lookup_raw = min(best_lookup_raw, start.elapsed());
        }
        println!(
            "Raw chDB lookup_with_params (RowBinary, final=1, {LOOKUPS} ops): {:.2?} ({:.2?}/op)",
            best_lookup_raw,
            best_lookup_raw / LOOKUPS as u32,
        );
    }

    fn write_rb_str(buffer: &mut Vec<u8>, string: &str) {
        let mut n = string.len();
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            buffer.push(b);
            if n == 0 {
                break;
            }
        }
        buffer.extend_from_slice(string.as_bytes());
    }
}
