<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-chdb

chDB driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for chDB, mapping Tank operations and queries into direct chDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- In-process SQL engine on top of ClickHouse via [`chdb-rust`](https://crates.io/crates/chdb-rust)
- Reuses the ClickHouse SQL dialect from [`tank-clickhouse`](https://crates.io/crates/tank-clickhouse)
- Streams results in `JSONEachRow` format, parsed row by row

## Install
```sh
cargo add tank
cargo add tank-chdb
```

Optional feature flags:
- `bundled` (default): links the chDB library statically, so the binary is self-contained.

Disable it to link `libchdb` dynamically instead:
```sh
cargo add tank-chdb --no-default-features
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_chdb::ChDBDriver;

let driver = ChDBDriver::new();
let pool = driver.connect_pool("chdb://".into(), PoolConfig::new()).await?;
let mut connection = pool.get().await?;
```
