//! SQL writer benchmark: time and heap allocations to render the common queries.
//!
//! Debug builds run a reduced workload as a smoke test; for meaningful numbers pin a core and run:
//! `taskset -c 2 cargo test --release -p tank-core --test benchmark -- --nocapture --test-threads=1`
#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        hint::black_box,
        sync::atomic::{AtomicUsize, Ordering},
        time::Instant,
    };
    use tank::{DynQuery, Entity, GenericSqlWriter, Interval, Sql, SqlWriter, cols, expr, join};
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
    use uuid::Uuid;

    /// Counts every allocation and reallocation, the writer should mostly append in place.
    struct CountingAllocator;

    static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    const ITERATIONS: usize = if cfg!(debug_assertions) { 10 } else { 2_000 };
    const SAMPLES: usize = if cfg!(debug_assertions) { 3 } else { 51 };
    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[derive(Entity, Clone)]
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
                account: (id % 512) as i32,
                symbol: ["AAPL", "MSFT", "NVDA", "BRK.B"][id % 4].into(),
                buy: id % 3 != 0,
                quantity: (id * 37 % 1000 + 1) as i32,
                price: 50.0 + (id * 7919 % 100_000) as f64 / 100.0,
                note: format!("order {id:08} routed via smart order router"),
                memo: (id % 4 == 0).then(|| format!("client's memo #{id}")),
                executed_at: PrimitiveDateTime::new(
                    Date::from_calendar_date(2026, Month::January, 2).unwrap(),
                    Time::from_hms_milli(9, 30, (id % 60) as u8, (id % 1000) as u16).unwrap(),
                ),
            }
        }
    }

    #[derive(Entity)]
    #[tank(name = "bench_accounts")]
    struct Account {
        #[tank(primary_key)]
        id: i32,
        name: String,
    }

    #[derive(Entity)]
    #[tank(schema = "market", name = "bench_wide")]
    struct Wide {
        #[tank(primary_key)]
        id: Uuid,
        small: i16,
        ratio: f32,
        amount: Decimal,
        flag: Option<bool>,
        #[tank(unique)]
        label: String,
        payload: Box<[u8]>,
        day: Date,
        at: Time,
        created: OffsetDateTime,
        ttl: Interval,
        missing: Option<i64>,
    }

    impl Wide {
        fn generate() -> Self {
            Self {
                id: Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
                small: -1234,
                ratio: 0.125,
                amount: Decimal::new(123_456_789, 4),
                flag: Some(true),
                label: "O'Brien's desk".into(),
                payload: (0..32u8).collect(),
                day: Date::from_calendar_date(2026, Month::March, 14).unwrap(),
                at: Time::from_hms_micro(13, 45, 7, 250_000).unwrap(),
                created: OffsetDateTime::new_in_offset(
                    Date::from_calendar_date(2025, Month::December, 31).unwrap(),
                    Time::from_hms_nano(23, 59, 59, 123_456_789).unwrap(),
                    UtcOffset::from_hms(-5, -30, 0).unwrap(),
                ),
                ttl: Interval::from_days(3) + Interval::from_secs(4 * 3600 + 30),
                missing: None,
            }
        }
    }

    struct Measure {
        name: &'static str,
        len: usize,
        fresh_ns: (f64, f64),
        fresh_allocs: f64,
        reused_ns: (f64, f64),
        reused_allocs: f64,
    }

    /// Returns (min, median) nanoseconds per operation across the samples.
    fn time(mut f: impl FnMut()) -> (f64, f64) {
        for _ in 0..ITERATIONS {
            f();
        }
        let mut samples = (0..SAMPLES)
            .map(|_| {
                let start = Instant::now();
                for _ in 0..ITERATIONS {
                    f();
                }
                start.elapsed().as_nanos() as f64 / ITERATIONS as f64
            })
            .collect::<Vec<_>>();
        samples.sort_by(f64::total_cmp);
        (samples[0], samples[SAMPLES / 2])
    }

    fn allocations(mut f: impl FnMut()) -> f64 {
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for _ in 0..ITERATIONS {
            f();
        }
        (ALLOCATIONS.load(Ordering::Relaxed) - before) as f64 / ITERATIONS as f64
    }

    /// Measures writing into a new `DynQuery` (as `build` does) and into a cleared, reused one.
    fn measure(name: &'static str, mut write: impl FnMut(&mut DynQuery)) -> Measure {
        let mut fresh = || {
            let mut out = DynQuery::default();
            write(&mut out);
            black_box(out);
        };
        let fresh_ns = time(&mut fresh);
        let fresh_allocs = allocations(&mut fresh);
        let mut out = DynQuery::default();
        let mut reused = || {
            out.clear();
            write(black_box(&mut out));
        };
        let reused_ns = time(&mut reused);
        let reused_allocs = allocations(&mut reused);
        let len = out.len();
        Measure {
            name,
            len,
            fresh_ns,
            fresh_allocs,
            reused_ns,
            reused_allocs,
        }
    }

    #[test]
    fn sql_writer_benchmark() {
        let trades = (0..100).map(Trade::generate).collect::<Vec<_>>();
        let trade = &trades[4];
        let wide = Wide::generate();
        let account = black_box(42);
        let results = [
            measure("select_find", |out| {
                WRITER.write_select(
                    out,
                    Sql.select(Trade::columns())
                        .from(Trade::table())
                        .where_expr(expr!(Trade::account == #account && Trade::buy == true))
                        .limit(Some(16)),
                )
            }),
            measure("select_prepared", |out| {
                WRITER.write_select(
                    out,
                    Sql.select(Trade::columns())
                        .from(Trade::table())
                        .where_expr(expr!(Trade::id == ? && Trade::symbol == ?))
                        .limit(Some(1)),
                )
            }),
            measure("select_aggregate", |out| {
                WRITER.write_select(
                    out,
                    Sql.select(cols!(
                        Trade::symbol,
                        COUNT(*),
                        SUM(Trade::quantity) as total,
                        AVG(Trade::price)
                    ))
                    .from(Trade::table())
                    .where_expr(expr!(Trade::buy == true && Trade::price >= 75.25))
                    .group_by(cols!(Trade::symbol))
                    .having(expr!(COUNT(*) > 5))
                    .order_by(cols!(SUM(Trade::quantity) DESC, Trade::symbol ASC))
                    .limit(Some(100)),
                )
            }),
            measure("select_join", |out| {
                WRITER.write_select(
                    out,
                    Sql.select(cols!(Trade::id, Account::name, Trade::price))
                        .from(join!(Trade LEFT JOIN Account ON Trade::account == Account::id))
                        .where_expr(expr!(
                            Trade::symbol == ("AAPL", "MSFT", "NVDA") as IN
                                && Account::name != "closed"
                        ))
                        .order_by(cols!(Trade::price DESC)),
                )
            }),
            measure("insert_one", |out| {
                WRITER.write_insert(out, Sql.insert_into(Trade::table()).values([trade]))
            }),
            measure("insert_many_100", |out| {
                WRITER.write_insert(out, Sql.insert_into(Trade::table()).values(&trades))
            }),
            measure("upsert", |out| {
                WRITER.write_insert(
                    out,
                    Sql.insert_into(Trade::table()).values([trade]).update(),
                )
            }),
            measure("insert_wide", |out| {
                WRITER.write_insert(out, Sql.insert_into(Wide::table()).values([&wide]))
            }),
            measure("delete", |out| {
                WRITER.write_delete(
                    out,
                    Trade::table(),
                    expr!(Trade::account >= 500 && Trade::symbol != "AAPL"),
                )
            }),
            measure("create_table", |out| {
                WRITER.write_create_table::<Wide>(out, true)
            }),
            measure("drop_table", |out| {
                WRITER.write_drop_table::<Wide>(out, true)
            }),
        ];
        println!(
            "SQL writer benchmark: {SAMPLES} samples x {ITERATIONS} iterations\n\
             {:<18} {:>6} {:>11} {:>11} {:>8} {:>11} {:>11} {:>8}",
            "query", "len", "fresh min", "fresh med", "allocs", "reuse min", "reuse med", "allocs"
        );
        for m in &results {
            println!(
                "{:<18} {:>6} {:>9.1}ns {:>9.1}ns {:>8.2} {:>9.1}ns {:>9.1}ns {:>8.2}",
                m.name,
                m.len,
                m.fresh_ns.0,
                m.fresh_ns.1,
                m.fresh_allocs,
                m.reused_ns.0,
                m.reused_ns.1,
                m.reused_allocs,
            );
        }
    }
}
