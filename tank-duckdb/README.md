<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-duckdb

`tank-duckdb` is the DuckDB driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native DuckDB commands. Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-duckdb

## Features
- DuckDB C API (FFI) using [libduckdb-sys](https://crates.io/crates/libduckdb-sys)
- Bulk inserts use DuckDB's appender API
- Queries execute through the [Tokio](https://crates.io/crates/tokio) runtime, with results streamed through a [Flume](https://crates.io/crates/flume) channel

## Install
```sh
cargo add tank
cargo add tank-duckdb
```

Optional feature flags:
- `bundled` (default): uses the bundled DuckDB library.

Disable it if you want a system DuckDB:
```sh
cargo add tank-duckdb --no-default-features
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_duckdb::DuckDBDriver;

let driver = DuckDBDriver::new();
let pool = driver
    .connect_pool("duckdb://path/to/database.duckdb?mode=rw".into(), PoolConfig::new())
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.
