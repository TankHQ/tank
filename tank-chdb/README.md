<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-chdb

chDB driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for chDB, mapping Tank operations and queries into direct chDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank-chdb

## Features
- In-process SQL engine on top of ClickHouse via [`chdb-rust`](https://crates.io/crates/chdb-rust)
- Reuses the ClickHouse SQL dialect from [`tank-clickhouse`](https://crates.io/crates/tank-clickhouse)
- Streams results in `JSONCompact` chunks with column metadata, with a non-streaming fallback for statements rejected as unsupported by the native streaming API

## Install
```sh
cargo add tank
cargo add tank-chdb
```

Optional feature flags:
- `bundled` (default): statically links `libchdb`, not the entire application binary. Native system and C++ runtime dependencies still apply.

Tank pins `chdb-rust` to `1.4.0`. Its build script uses an existing native library and header when available, otherwise downloads chDB core `26.5.0` artifacts. Automatic downloads cover Linux and macOS on x86-64 and AArch64 (Apple Silicon on macOS). Building requires `libclang` for bindgen and native linking dependencies: `libstdc++` on Linux; `libc++`, `iconv`, and the CoreFoundation and Security frameworks on macOS. A first build may need network access to download the artifacts. The upstream downloader uses native TLS; that build-time dependency and TLS internal to the native engine are not controlled by Tank's transport settings.

Disable it to link `libchdb` dynamically instead:
```sh
cargo add tank-chdb --no-default-features
```

Dynamic linking requires a compatible shared `libchdb` at build time and on the runtime library search path. Its C API must include the streaming and result-management functions used by this driver; an older library without those symbols is insufficient.

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_chdb::ChDBDriver;

let driver = ChDBDriver::new();
let pool = driver.connect_pool("chdb://", PoolConfig::new()).await?;
let mut connection = pool.get().await?;
```

## Connection
**URL Format**:
- Memory: `chdb://`
- Persistent database directory: `chdb://?path=/absolute/path/to/database`

The first nonempty, case-insensitive `path` query parameter takes precedence. Otherwise, the driver uses the URL path with leading slashes stripped, so `chdb:///relative/database` selects a relative directory. Use `?path=` for an absolute path. The URL authority and other query parameters are ignored. Without a path, the database is in memory and lasts for the connection's lifetime.

On connect, Tank applies `allow_experimental_lightweight_delete=1`, `join_use_nulls=1`, `final=1`, and `output_format_json_quote_decimals=1`. If any session setting fails, the connection attempt fails.

## Limitations
- Transactions and multi-statement execution are unsupported. `begin()` returns an error.
- `append()` uses SQL `INSERT`, not a specialized ingestion API. Affected-row counts are unknown (`None`), and generated IDs are not returned.
- Preparation is emulated, not a server-side prepared statement or wire-level parameter binding. See [raw SQL and parameters](../docs/09-raw-sql.md#clickhouse-and-chdb).
- Generated tables use `ReplacingMergeTree()` with entity primary keys as the `ORDER BY` key; without a primary key, they use `MergeTree()` with eligible non-nullable scalar columns as the sorting key, or `tuple()` if there are none. `save()` emits an `INSERT`, not an atomic UPSERT. Replacing-engine deduplication is not enforced uniqueness; ordinary `MergeTree()` does not deduplicate.
- Generated DDL omits unique and foreign-key constraints, defaults, and comments. A Tank schema is a ClickHouse database. Column type overrides use the `clickhouse` key, not `chdb`.
- Results require schema-bearing `JSONCompact` output. Avoid explicit `FORMAT` clauses that omit metadata or change the expected result structure. Non-streaming fallback results are materialized in full.
- The async interface is not fully nonblocking: opening a connection and initializing a query use blocking workers, but subsequent native chunk fetches can block while polling the stream, and cancellation/cleanup can block when it is dropped.

See [ClickHouse/chDB capabilities](../docs/04-capabilities.md#clickhouse-and-chdb) for shared semantics and [type mappings](../docs/05-types.md#clickhouse-and-chdb) for conversion limits.

## Running Tests
chDB tests run embedded: this driver's tests need no database server or Docker, but do require the native build dependencies above and a compatible runtime library when dynamically linked.

From the repository root, run the full workspace validation with the pinned toolchain:
```sh
rustup run nightly-2026-10-12 ./test.sh
```

This runs `cargo check` and each workspace member's tests separately, preserving driver-specific feature combinations. The full suite also needs the other drivers' prerequisites, including [Docker with `TANK_CLICKHOUSE_TEST` unset](../tank-clickhouse/README.md#running-tests). A package-only test run does not replace this validation.
