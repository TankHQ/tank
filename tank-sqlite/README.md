<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-sqlite

SQLite driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for SQLite, mapping Tank operations and queries into direct SQLite commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

⭐ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- SQLite C API (FFI) using [libsqlite3-sys](https://crates.io/crates/libsqlite3-sys)
- Queries are streamed row by row using `try_stream!` ([async_stream](https://crates.io/crates/async-stream)): each statement is stepped with `sqlite3_step` and results are yielded immediately (no buffering occurs)

## Install
```sh
cargo add tank
cargo add tank-sqlite
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_sqlite::SQLiteDriver;

let driver = SQLiteDriver::new();
let connection = driver
    .connect("sqlite://path/to/database.sqlite?mode=rw".into())
    .await?;
```
