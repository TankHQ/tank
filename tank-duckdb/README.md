<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-duckdb

DuckDB driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for DuckDB, mapping Tank operations and queries into direct DuckDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. you still use it to define entities, manage schemas, and build queries.

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

## Features
- DuckDB C API (FFI) using [libduckdb-sys](https://crates.io/crates/libduckdb-sys)
- Bulk inserts use DuckDB's appender API
- Queries are executed in parallel using [tokio](https://crates.io/crates/tokio) runtime, results are send using [flume](https://crates.io/crates/flume) unbounded channel

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
use tank::{Connection, Driver, Executor};
use tank_duckdb::DuckDBDriver;

let driver = DuckDBDriver::new();
let connection = driver
    .connect("duckdb://path/to/database.duckdb?mode=rw".into())
    .await?;
```
