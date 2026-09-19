<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-sqlite

`tank-sqlite` is the SQLite driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native SQLite commands. Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-sqlite

## Features
- SQLite C API (FFI) using [libsqlite3-sys](https://crates.io/crates/libsqlite3-sys)
- Queries stream row by row through [`async-stream`](https://crates.io/crates/async-stream), with each statement stepped using `sqlite3_step` and no result buffering

## Install
```sh
cargo add tank
cargo add tank-sqlite
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_sqlite::SQLiteDriver;

let driver = SQLiteDriver::new();
let pool = driver
    .connect_pool("sqlite://path/to/database.sqlite?mode=rw".into(), PoolConfig::new())
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.
