//! Mixed-workload benchmark measuring the driver and ORM overhead around chDB.
//!
//! Debug builds run a reduced workload as a smoke test. For meaningful numbers run:
//! `cargo test --release -p tank-chdb --test benchmark -- --nocapture`
#[cfg(test)]
mod tests {
    use chdb_rust::{
        connection::Connection as RawConnection,
        format::{InputFormat, OutputFormat},
    };
    use std::{
        cmp::min,
        time::{Duration, Instant},
    };
    use tank::Connection;
    use tank_chdb::{ChDBConnection, ChDBDriver};
    use tank_tests::{
        BenchmarkConfig, CountingAllocator, Measurement, benchmark, benchmark_spread, init_logs,
    };

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    #[tokio::test]
    async fn chdb_benchmark() {
        init_logs();
        let mut connection = ChDBConnection::connect(&ChDBDriver::new(), "chdb://".into())
            .await
            .unwrap();
        let scale = if cfg!(debug_assertions) { 1 } else { 10 };
        let config = BenchmarkConfig {
            rounds: if cfg!(debug_assertions) { 1 } else { 5 },
            lookups: 50 * scale,
            single_writes: 20 * scale,
            mixed: 50 * scale,
            max_raw_stream_allocs: 10_000 * scale / 50,
            ..BenchmarkConfig::default()
        };
        let trades = benchmark(&mut connection, &config).await;

        // Outside timed phases: isolate engine execution, format serialization, and
        // streaming insert cost from Tank's adapter overhead on the same table.
        let mut raw = RawConnection::open_in_memory().unwrap();
        raw.query("SET final=1", OutputFormat::Null).unwrap();
        println!(
            "\nRaw chDB scan baseline ({} rows, final=1, best of {} rounds):",
            config.rows, config.rounds
        );
        for (label, format) in [
            ("engine only (Null)", OutputFormat::Null),
            ("JSONCompact", OutputFormat::JSONCompact),
            ("RowBinary", OutputFormat::RowBinary),
            ("Native", OutputFormat::Native),
            ("ArrowStream", OutputFormat::ArrowStream),
        ] {
            let mut best = Duration::MAX;
            let mut payload_bytes = 0;
            for _ in 0..config.rounds {
                let start = Instant::now();
                let res = raw.query("SELECT * FROM bench_trades", format).unwrap();
                let elapsed = start.elapsed();
                payload_bytes = res.data_ref().len();
                best = min(best, elapsed);
            }
            println!(
                "  query  {label:<20} {:>10} ({:>10}/row, {:>8.1} KiB)",
                format!("{best:.2?}"),
                format!("{:.2?}", best / config.rows as u32),
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
            for _ in 0..config.rounds {
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
                format!("{:.2?}", best / config.rows as u32),
            );
        }
        {
            let mut best = Duration::MAX;
            let mut chunks = 0usize;
            for _ in 0..config.rounds {
                let start = Instant::now();
                let mut stream = raw
                    .query_stream_arrow("SELECT * FROM bench_trades")
                    .unwrap();
                let mut c = 0;
                while let Some(_batch) = stream.next_batch().unwrap() {
                    c += 1;
                }
                chunks = c;
                best = min(best, start.elapsed());
            }
            println!(
                "  stream {:<20} {:>10} ({:>10}/row, {chunks} chunks)",
                "Arrow C Data",
                format!("{best:.2?}"),
                format!("{:.2?}", best / config.rows as u32),
            );
        }

        // Compare direct RowBinary insert_stream vs Tank's Arrow append.
        let mut best_insert_stream = Duration::MAX;
        let mut best_insert_allocs = usize::MAX;
        let mut best_insert_bytes = usize::MAX;
        for _ in 0..config.rounds {
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
            assert_eq!(stats.rows_written, config.rows as u64);
            best_insert_stream = min(best_insert_stream, elapsed);
            best_insert_allocs = min(best_insert_allocs, allocs);
            best_insert_bytes = min(best_insert_bytes, bytes);
        }
        println!(
            "\nRaw chDB insert_stream (RowBinary, {} rows): {:.2?} ({:.2?}/row, {:.2} allocs/row, {:.0} bytes/row)",
            config.rows,
            best_insert_stream,
            best_insert_stream / config.rows as u32,
            best_insert_allocs as f64 / config.rows as f64,
            best_insert_bytes as f64 / config.rows as f64,
        );

        let mut best_lookup_raw = Duration::MAX;
        for _ in 0..config.rounds {
            let start = Instant::now();
            for i in 0..config.lookups {
                let id = benchmark_spread(i, config.rows);
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
            "Raw chDB lookup_with_params (RowBinary, final=1, {} ops): {:.2?} ({:.2?}/op)",
            config.lookups,
            best_lookup_raw,
            best_lookup_raw / config.lookups as u32,
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
