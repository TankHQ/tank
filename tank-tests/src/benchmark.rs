use std::{
    alloc::{GlobalAlloc, Layout, System},
    cmp::min,
    fmt::{self, Display, Formatter},
    future::ready,
    pin::pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
#[cfg(not(feature = "disable-transactions"))]
use tank::Transaction;
use tank::{
    AsValue, Connection, Driver, Entity, Executor, Row, Sql, cols, expr, stream::TryStreamExt,
};
use time::{Duration as TimeDuration, PrimitiveDateTime, macros::datetime};

const SCALE: usize = if cfg!(debug_assertions) { 1 } else { 10 };
const ACCOUNTS: usize = 512;
const SYMBOLS: [&str; 16] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK.B", "JPM", "V", "XOM",
    "UNH", "MA", "COST", "HD",
];

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Counts Rust heap allocations and bytes when installed as `#[global_allocator]`.
pub struct CountingAllocator;

impl CountingAllocator {
    pub fn counts() -> (usize, usize) {
        (
            ALLOCATIONS.load(Ordering::Relaxed),
            ALLOCATED_BYTES.load(Ordering::Relaxed),
        )
    }
}

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

/// Captures wall time, allocation count, and allocated bytes for one benchmark phase.
pub struct Measurement {
    allocations: usize,
    bytes: usize,
    start: Instant,
}

impl Measurement {
    pub fn start() -> Self {
        let (allocations, bytes) = CountingAllocator::counts();
        Self {
            allocations,
            bytes,
            start: Instant::now(),
        }
    }

    pub fn finish(self) -> (Duration, usize, usize) {
        let (allocations, bytes) = CountingAllocator::counts();
        (
            self.start.elapsed(),
            allocations - self.allocations,
            bytes - self.bytes,
        )
    }
}

/// Workload sizes for [`benchmark`].
#[derive(Debug, Clone, Copy)]
pub struct BenchmarkConfig {
    pub rows: usize,
    pub rounds: usize,
    pub lookups: usize,
    pub ranges: usize,
    pub filters: usize,
    pub aggregates: usize,
    pub single_writes: usize,
    pub mixed: usize,
    pub max_raw_stream_allocs: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        let rows = 10_000 * SCALE;
        Self {
            rows,
            rounds: if cfg!(debug_assertions) { 1 } else { 7 },
            lookups: 500 * SCALE,
            ranges: 20 * SCALE,
            filters: 10 * SCALE,
            aggregates: 5 * SCALE,
            single_writes: 50 * SCALE,
            mixed: 200 * SCALE,
            max_raw_stream_allocs: rows / 100,
        }
    }
}

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(name = "bench_trades")]
pub struct BenchTrade {
    #[tank(primary_key)]
    pub id: i64,
    pub account: i32,
    pub symbol: String,
    pub buy: bool,
    pub quantity: i32,
    pub price: f64,
    pub note: String,
    pub memo: Option<String>,
    pub executed_at: PrimitiveDateTime,
}

impl BenchTrade {
    pub fn generate(id: usize) -> Self {
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

/// Deterministic, well-spread sequence of ids in `0..rows`.
pub fn benchmark_spread(i: usize, rows: usize) -> i64 {
    (i * 7919 % rows) as i64
}

struct Report {
    driver: &'static str,
    rows: usize,
    rounds: usize,
    phases: Vec<(&'static str, usize, Duration, usize, usize)>,
}

impl Report {
    fn new(driver: &'static str, config: &BenchmarkConfig) -> Self {
        Self {
            driver,
            rows: config.rows,
            rounds: config.rounds,
            phases: Vec::new(),
        }
    }

    fn record(&mut self, name: &'static str, ops: usize, sample: (Duration, usize, usize)) {
        let (elapsed, allocations, bytes) = sample;
        match self.phases.iter_mut().find(|(n, ..)| *n == name) {
            Some((_, _, best, best_allocs, best_bytes)) => {
                *best = min(*best, elapsed);
                *best_allocs = min(*best_allocs, allocations);
                *best_bytes = min(*best_bytes, bytes);
            }
            None => self.phases.push((name, ops, elapsed, allocations, bytes)),
        }
    }
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} benchmark: {} rows, best of {} rounds\n\
             {:<20} {:>8} {:>12} {:>12} {:>10} {:>12}",
            self.driver,
            self.rows,
            self.rounds,
            "phase",
            "ops",
            "total",
            "per op",
            "allocs/op",
            "bytes/op"
        )?;
        let mut total = Duration::ZERO;
        for &(name, ops, elapsed, allocations, bytes) in &self.phases {
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

async fn round<C: Connection>(
    connection: &mut C,
    trades: &[BenchTrade],
    config: &BenchmarkConfig,
    report: &mut Report,
) {
    let rows = config.rows;
    BenchTrade::drop_table(connection, true, false)
        .await
        .unwrap();
    BenchTrade::create_table(connection, true, false)
        .await
        .unwrap();

    // Bulk load through `append`.
    let m = Measurement::start();
    let affected = BenchTrade::insert_many(connection, trades).await.unwrap();
    report.record("append", rows, m.finish());
    if let Some(count) = affected.rows_affected {
        assert_eq!(count, rows as u64);
    }

    // Full table scan decoded into entities.
    let m = Measurement::start();
    let loaded: Vec<BenchTrade> = BenchTrade::find_many(connection, true, None)
        .try_collect()
        .await
        .unwrap();
    report.record("scan", rows, m.finish());
    assert_eq!(loaded.len(), rows);
    let mid = (rows / 2) as i64;
    assert_eq!(loaded.iter().find(|t| t.id == mid), Some(&trades[rows / 2]));

    // Ad hoc filtered queries, rendered and parsed each time.
    let m = Measurement::start();
    let mut found = 0;
    for i in 0..config.filters {
        let account = (i * 13 % ACCOUNTS) as i32;
        found += BenchTrade::find_many(
            connection,
            expr!(BenchTrade::account == #account && BenchTrade::buy == true),
            None,
        )
        .try_fold(0, |n, _| ready(Ok(n + 1)))
        .await
        .unwrap();
    }
    report.record("filter", config.filters, m.finish());
    assert!(found > 0);

    // Prepared point lookups by primary key.
    let mut by_id = BenchTrade::prepare_find(connection, expr!(BenchTrade::id == ?), Some(1))
        .await
        .unwrap();
    let m = Measurement::start();
    for i in 0..config.lookups {
        let id = benchmark_spread(i, rows);
        by_id.bind(id).unwrap();
        let trade: Vec<BenchTrade> = connection
            .fetch(&mut by_id)
            .and_then(|row| ready(BenchTrade::from_row(row)))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(trade[0].id, id);
    }
    report.record("lookup_prepared", config.lookups, m.finish());

    // Prepared range queries binding a string and an integer.
    let mut by_symbol = BenchTrade::prepare_find(
        connection,
        expr!(BenchTrade::symbol == ? && BenchTrade::account < ?),
        None,
    )
    .await
    .unwrap();
    let m = Measurement::start();
    for i in 0..config.ranges {
        let symbol = SYMBOLS[i % SYMBOLS.len()];
        by_symbol.bind(symbol).unwrap().bind(64).unwrap();
        let found_trades: Vec<BenchTrade> = connection
            .fetch(&mut by_symbol)
            .and_then(|row| ready(BenchTrade::from_row(row)))
            .try_collect()
            .await
            .unwrap();
        assert!(
            found_trades
                .iter()
                .all(|t| t.symbol == symbol && t.account < 64)
        );
    }
    report.record("range_prepared", config.ranges, m.finish());

    // Aggregation decoded through the raw row interface.
    let agg_query = Sql()
        .select(cols!(
            BenchTrade::symbol,
            COUNT(*),
            SUM(BenchTrade::quantity),
            AVG(BenchTrade::price)
        ))
        .from(BenchTrade::table())
        .where_expr(expr!(BenchTrade::buy == true))
        .group_by(cols!(BenchTrade::symbol))
        .build(&connection.driver());
    let agg_sql = agg_query.as_str();
    let m = Measurement::start();
    for _ in 0..config.aggregates {
        let groups = connection
            .fetch(&*agg_sql)
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
    report.record("aggregate", config.aggregates, m.finish());

    // Single-row writes going through the SQL writer.
    let m = Measurement::start();
    for i in 0..config.single_writes {
        BenchTrade::insert_one(connection, &BenchTrade::generate(rows + i))
            .await
            .unwrap();
    }
    report.record("insert_one", config.single_writes, m.finish());

    let m = Measurement::start();
    for i in 0..config.single_writes {
        let mut trade = trades[benchmark_spread(i, rows) as usize].clone();
        trade.quantity += 1;
        trade.memo = Some("amended".into());
        trade.save(connection).await.unwrap();
    }
    report.record("save", config.single_writes, m.finish());

    // Interleaved workload resembling an application serving requests.
    let m = Measurement::start();
    let mut next_id = rows + config.single_writes;
    for i in 0..config.mixed {
        by_id.bind(benchmark_spread(i, rows)).unwrap();
        let trade = pin!(
            connection
                .fetch(&mut by_id)
                .and_then(|row| ready(BenchTrade::from_row(row)))
        )
        .try_next()
        .await
        .unwrap()
        .unwrap();
        match i % 10 {
            0 => {
                let batch = (next_id..next_id + 32)
                    .map(BenchTrade::generate)
                    .collect::<Vec<_>>();
                next_id += batch.len();
                BenchTrade::insert_many(connection, &batch).await.unwrap();
            }
            3 => {
                let mut trade = trade;
                trade.price *= 1.01;
                trade.save(connection).await.unwrap();
            }
            7 => {
                let account = trade.account;
                BenchTrade::find_many(connection, expr!(BenchTrade::account == #account), Some(16))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    report.record("mixed", config.mixed, m.finish());

    // Bulk maintenance (inside a transaction when supported).
    let m = Measurement::start();
    #[cfg(not(feature = "disable-transactions"))]
    {
        let mut tx = connection.begin().await.unwrap();
        let updated = tx
            .execute(
                Sql()
                    .update(BenchTrade::table())
                    .set(cols!(BenchTrade::price == BenchTrade::price * 1.001))
                    .where_expr(expr!(BenchTrade::buy == true))
                    .build(&tx.driver()),
            )
            .await
            .unwrap();
        let deleted = BenchTrade::delete_many(&mut tx, expr!(BenchTrade::account >= 500))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        report.record("transaction", 1, m.finish());
        assert!(updated.rows_affected.unwrap() > 0 && deleted.rows_affected.unwrap() > 0);
    }
    #[cfg(feature = "disable-transactions")]
    {
        connection
            .execute(
                Sql()
                    .update(BenchTrade::table())
                    .set(cols!(BenchTrade::price == BenchTrade::price * 1.001))
                    .where_expr(expr!(BenchTrade::buy == true))
                    .build(&connection.driver()),
            )
            .await
            .unwrap();
        BenchTrade::delete_many(connection, expr!(BenchTrade::account >= 500))
            .await
            .unwrap();
        report.record("maintenance", 1, m.finish());
    }
}

/// Runs the shared mixed-workload benchmark against `connection`, prints the report,
/// and leaves `bench_trades` populated with the returned rows for optional raw baselines.
pub async fn benchmark<C: Connection>(
    connection: &mut C,
    config: &BenchmarkConfig,
) -> Vec<BenchTrade> {
    let trades = (0..config.rows)
        .map(BenchTrade::generate)
        .collect::<Vec<_>>();
    let mut report = Report::new(connection.driver().name(), config);
    for _ in 0..config.rounds {
        round(connection, &trades, config, &mut report).await;
    }

    // Run separately so this diagnostic does not warm caches for the original phases.
    BenchTrade::drop_table(connection, true, false)
        .await
        .unwrap();
    BenchTrade::create_table(connection, true, false)
        .await
        .unwrap();
    BenchTrade::insert_many(connection, &trades).await.unwrap();
    let scan_query = Sql()
        .select(BenchTrade::columns())
        .from(BenchTrade::table())
        .build(&connection.driver());
    let scan_sql = scan_query.as_str();
    for _ in 0..config.rounds {
        let m = Measurement::start();
        let sum = connection
            .fetch(&*scan_sql)
            .try_fold(0i64, |sum, row| {
                ready(i64::decode_value(row.value(0).unwrap()).map(|id| sum + id))
            })
            .await
            .unwrap();
        let sample = m.finish();
        report.record("raw_stream", config.rows, sample);
        assert_eq!(sum, (config.rows * (config.rows - 1) / 2) as i64);
        assert!(
            sample.1 < config.max_raw_stream_allocs,
            "{} allocations for {} rows",
            sample.1,
            config.rows,
        );
    }
    println!("{report}");
    trades
}
