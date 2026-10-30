<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-clickhouse

ClickHouse driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for ClickHouse, mapping Tank operations and queries into direct ClickHouse commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- Async connection and execution via [`klickhouse`](https://crates.io/crates/klickhouse)
- Streams result rows block by block using `try_stream!` ([async_stream](https://crates.io/crates/async-stream))

## Install
```sh
cargo add tank
cargo add tank-clickhouse
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_clickhouse::ClickHouseDriver;

let driver = ClickHouseDriver::new();
let pool = driver
    .connect_pool(
        "clickhouse://default@127.0.0.1:9000/default".into(),
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

## Running Tests
Tests need a ClickHouse instance. Provide a connection URL via `TANK_CLICKHOUSE_TEST`. If absent, a containerized ClickHouse will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

1. Ensure Docker is running (linux):
```sh
systemctl status docker
```
2. Add your user to the `docker` group if needed (linux):
```sh
sudo usermod -aG docker $USER
```

> [!CAUTION]
> Avoid aborting tests mid-run (e.g. killing the process at a breakpoint). Containers might be left running and consume resources.
>
> List containers:
> ```sh
> docker ps
> ```
> Stop container:
> ```sh
> docker kill <container_id_or_name>
> ```
