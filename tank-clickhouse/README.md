<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-clickhouse

ClickHouse driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for ClickHouse, mapping Tank operations and queries into direct ClickHouse commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank-clickhouse

## Features
- Async connection and execution over the native ClickHouse protocol via [`klickhouse`](https://crates.io/crates/klickhouse)
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
        "clickhouse://default@127.0.0.1:9000/default",
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

## Connection
**URL Format**: `clickhouse://user:password@host:port/database`

The driver uses plaintext native TCP, not HTTP, and exposes no TLS configuration. A hostname and explicit native port (normally `9000`) are required; no port is supplied automatically. Omitted credentials use user `default` and an empty password. An omitted database is left unspecified in the client options, so the server chooses its default.

The only supported query parameter is `tcp_nodelay`, which defaults to `true` and accepts only `true` or `false`. Unknown parameters and invalid boolean values return errors. Supply credentials and the database in the URL authority and path, not query parameters.

On connect, Tank applies `allow_experimental_lightweight_delete=1`, `join_use_nulls=1`, and `final=1`. If any session setting fails, the connection attempt fails.

## Limitations
- Transactions and multi-statement execution are unsupported. `begin()` returns an error.
- `append()` uses SQL `INSERT`, not a specialized ingestion API. Affected-row counts are unknown (`None`), and generated IDs are not returned.
- Preparation is emulated, not a server-side prepared statement or wire-level parameter binding. See [raw SQL and parameters](../docs/09-raw-sql.md#clickhouse-and-chdb).
- Generated tables use `ReplacingMergeTree()`. `save()` emits an `INSERT`, not an atomic UPSERT; deduplication is an engine/read behavior, not enforced uniqueness. Entity primary keys select the `ORDER BY` key. Without a primary key, eligible non-nullable scalar columns form the sorting key, or `tuple()` if there are none.
- Generated DDL omits unique and foreign-key constraints, defaults, and comments. A Tank schema is a ClickHouse database. Column type overrides use the `clickhouse` key.

See [ClickHouse/chDB capabilities](../docs/04-capabilities.md#clickhouse-and-chdb) for shared semantics and [type mappings](../docs/05-types.md#clickhouse-and-chdb) for conversion limits.

## Running Tests
Tests currently require Docker and automatically launch ClickHouse using [testcontainers-modules](https://crates.io/crates/testcontainers-modules). Keep `TANK_CLICKHOUSE_TEST` unset: although the initializer reads this URL override, current integration test callers require a container handle and fail when it is absent.

1. Ensure Docker is running (linux):
```sh
systemctl status docker
```
2. Add your user to the `docker` group if needed (linux):
```sh
sudo usermod -aG docker $USER
```

From the repository root, run the full workspace validation with the pinned toolchain:
```sh
env -u TANK_CLICKHOUSE_TEST rustup run nightly-2026-10-12 ./test.sh
```

This runs `cargo check` and each workspace member's tests separately, preserving driver-specific feature combinations. The other drivers' test prerequisites also apply; a package-only test run does not replace this validation.

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
