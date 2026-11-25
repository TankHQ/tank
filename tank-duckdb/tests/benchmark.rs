//! Mixed-workload benchmark measuring the driver and ORM overhead around DuckDB.
//!
//! Debug builds run a reduced workload as a smoke test. For meaningful numbers run:
//! `cargo test --release -p tank-duckdb --test benchmark -- --nocapture`
#[cfg(test)]
mod tests {
    use libduckdb_sys::*;
    use std::{
        cmp::min,
        ffi::CString,
        mem, ptr,
        time::{Duration, Instant},
    };
    use tank::Connection;
    use tank_duckdb::{DuckDBConnection, DuckDBDriver};
    use tank_tests::{BenchmarkConfig, CountingAllocator, benchmark, benchmark_spread, init_logs};

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    #[tokio::test]
    async fn duckdb_benchmark() {
        init_logs();
        // A single DuckDB thread keeps timings stable, isolating the overhead outside the engine.
        let mut connection =
            DuckDBConnection::connect(&DuckDBDriver::new(), "duckdb://:memory:?threads=1".into())
                .await
                .unwrap();
        let config = BenchmarkConfig::default();
        let trades = benchmark(&mut connection, &config).await;

        // Outside timed phases: isolate DuckDB C API cost from Tank's driver overhead.
        unsafe {
            let mut db = ptr::null_mut();
            let mut conn = ptr::null_mut();
            let mut cfg = ptr::null_mut();
            duckdb_create_config(&mut cfg);
            duckdb_set_config(cfg, c"threads".as_ptr(), c"1".as_ptr());
            duckdb_open_ext(c":memory:".as_ptr(), &mut db, cfg, ptr::null_mut());
            duckdb_destroy_config(&mut cfg);
            duckdb_connect(db, &mut conn);
            let create_sql = CString::new(
                "CREATE TABLE bench_trades (id BIGINT PRIMARY KEY, account INTEGER, \
                 symbol VARCHAR, buy BOOLEAN, quantity INTEGER, price DOUBLE, \
                 note VARCHAR, memo VARCHAR, executed_at TIMESTAMP)",
            )
            .unwrap();
            duckdb_query(conn, create_sql.as_ptr(), ptr::null_mut());

            // Raw DuckDB C API data-chunk append baseline.
            let mut types = [
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR),
                duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP),
            ];
            let mut chunk = duckdb_create_data_chunk(types.as_mut_ptr(), types.len() as _);
            let batch_size = duckdb_vector_size() as usize;
            let mut best_append = Duration::MAX;
            for _ in 0..config.rounds {
                duckdb_query(conn, c"DELETE FROM bench_trades".as_ptr(), ptr::null_mut());
                let start = Instant::now();
                let mut appender = ptr::null_mut();
                duckdb_appender_create(conn, ptr::null(), c"bench_trades".as_ptr(), &mut appender);
                for slice in trades.chunks(batch_size) {
                    duckdb_data_chunk_reset(chunk);
                    let v0 = duckdb_data_chunk_get_vector(chunk, 0);
                    let v1 = duckdb_data_chunk_get_vector(chunk, 1);
                    let v2 = duckdb_data_chunk_get_vector(chunk, 2);
                    let v3 = duckdb_data_chunk_get_vector(chunk, 3);
                    let v4 = duckdb_data_chunk_get_vector(chunk, 4);
                    let v5 = duckdb_data_chunk_get_vector(chunk, 5);
                    let v6 = duckdb_data_chunk_get_vector(chunk, 6);
                    let v7 = duckdb_data_chunk_get_vector(chunk, 7);
                    let v8 = duckdb_data_chunk_get_vector(chunk, 8);
                    let d0 = duckdb_vector_get_data(v0) as *mut i64;
                    let d1 = duckdb_vector_get_data(v1) as *mut i32;
                    let d3 = duckdb_vector_get_data(v3) as *mut bool;
                    let d4 = duckdb_vector_get_data(v4) as *mut i32;
                    let d5 = duckdb_vector_get_data(v5) as *mut f64;
                    let d8 = duckdb_vector_get_data(v8) as *mut i64;
                    let mut val7: *mut u64 = ptr::null_mut();
                    for (r, t) in slice.iter().enumerate() {
                        *d0.add(r) = t.id;
                        *d1.add(r) = t.account;
                        duckdb_unsafe_vector_assign_string_element_len(
                            v2,
                            r as _,
                            t.symbol.as_ptr() as _,
                            t.symbol.len() as _,
                        );
                        *d3.add(r) = t.buy;
                        *d4.add(r) = t.quantity;
                        *d5.add(r) = t.price;
                        duckdb_unsafe_vector_assign_string_element_len(
                            v6,
                            r as _,
                            t.note.as_ptr() as _,
                            t.note.len() as _,
                        );
                        match &t.memo {
                            Some(m) => duckdb_unsafe_vector_assign_string_element_len(
                                v7,
                                r as _,
                                m.as_ptr() as _,
                                m.len() as _,
                            ),
                            None => {
                                if val7.is_null() {
                                    duckdb_vector_ensure_validity_writable(v7);
                                    val7 = duckdb_vector_get_validity(v7);
                                }
                                *val7.add(r >> 6) &= !(1 << (r & 63));
                            }
                        }
                        *d8.add(r) =
                            (t.executed_at.assume_utc().unix_timestamp_nanos() / 1_000) as i64;
                    }
                    duckdb_data_chunk_set_size(chunk, slice.len() as _);
                    duckdb_append_data_chunk(appender, chunk);
                }
                duckdb_appender_close(appender);
                duckdb_appender_destroy(&mut appender);
                best_append = min(best_append, start.elapsed());
            }
            duckdb_destroy_data_chunk(&mut chunk);
            for t in &mut types {
                duckdb_destroy_logical_type(t);
            }

            // Raw DuckDB C API scan baseline.
            let mut best_scan = Duration::MAX;
            let mut prep = ptr::null_mut();
            duckdb_prepare(conn, c"SELECT * FROM bench_trades".as_ptr(), &mut prep);
            for _ in 0..config.rounds {
                let start = Instant::now();
                let mut res = mem::zeroed();
                duckdb_execute_prepared_streaming(prep, &mut res);
                let mut sum = 0i64;
                loop {
                    let mut chunk = duckdb_fetch_chunk(res);
                    if chunk.is_null() {
                        break;
                    }
                    let size = duckdb_data_chunk_get_size(chunk) as usize;
                    let vec = duckdb_data_chunk_get_vector(chunk, 0);
                    let data = duckdb_vector_get_data(vec) as *const i64;
                    for r in 0..size {
                        sum += *data.add(r);
                    }
                    duckdb_destroy_data_chunk(&mut chunk);
                }
                best_scan = min(best_scan, start.elapsed());
                assert_eq!(sum, (config.rows * (config.rows - 1) / 2) as i64);
                duckdb_destroy_result(&mut res);
            }
            duckdb_destroy_prepare(&mut prep);

            // Raw DuckDB C API prepared point lookup baseline.
            let mut best_lookup = Duration::MAX;
            let mut lookup_prep = ptr::null_mut();
            duckdb_prepare(
                conn,
                c"SELECT * FROM bench_trades WHERE id = ? LIMIT 1".as_ptr(),
                &mut lookup_prep,
            );
            for _ in 0..config.rounds {
                let start = Instant::now();
                for i in 0..config.lookups {
                    let id = benchmark_spread(i, config.rows);
                    duckdb_bind_int64(lookup_prep, 1, id);
                    let mut res = mem::zeroed();
                    duckdb_execute_prepared_streaming(lookup_prep, &mut res);
                    let mut chunk = duckdb_fetch_chunk(res);
                    assert!(!chunk.is_null());
                    let vec = duckdb_data_chunk_get_vector(chunk, 0);
                    assert_eq!(*(duckdb_vector_get_data(vec) as *const i64), id);
                    duckdb_destroy_data_chunk(&mut chunk);
                    duckdb_destroy_result(&mut res);
                }
                best_lookup = min(best_lookup, start.elapsed());
            }
            duckdb_destroy_prepare(&mut lookup_prep);

            println!(
                "\nRaw DuckDB C API baseline ({} rows, best of {}):\n  \
                 append {:>10} ({:>10}/row)\n  \
                 scan   {:>10} ({:>10}/row)\n  \
                 lookup {:>10} ({:>10}/op)",
                config.rows,
                config.rounds,
                format!("{best_append:.2?}"),
                format!("{:.2?}", best_append / config.rows as u32),
                format!("{best_scan:.2?}"),
                format!("{:.2?}", best_scan / config.rows as u32),
                format!("{best_lookup:.2?}"),
                format!("{:.2?}", best_lookup / config.lookups as u32),
            );
            duckdb_disconnect(&mut conn);
            duckdb_close(&mut db);
        }
    }
}
