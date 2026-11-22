> [!NOTE]
> Just show me the code: [Cheat sheet](00-cheat-sheet.md)

# Introduction
###### *Field Manual Section 1* - Mission Briefing

Welcome to the Tank field manual. This is the quick guide for developers who want to drive, fight and survive with **Tank (Table Abstraction & Navigation Kit)**: the Rust data layer.

Tank lets you define a Rust struct once with `#[derive(Entity)]` and run typed, streaming queries across relational, analytical, document, wide-column, and key-value databases without rewriting your data model or giving up control over the queries being executed.

## Design Principles

- **One entity definition, zero setup**: Derive `Entity` on a plain Rust struct to get table creation, upserts, inserts, typed queries, and row decoding. No schema CLI or external build step required.
- **Explicit queries with `expr!`**: Filter, project, order, and join using typed Rust expressions compiled into an AST that each driver translates into its native dialect. Drop down to raw SQL or CQL whenever you want.
- **Streaming by default**: Query results stream asynchronously rather than buffering whole result sets up front. Where a database provides native bulk ingestion (Postgres, SQLite, DuckDB), Tank uses it.
- **Extensible driver model**: Database support lives behind `Driver`, `Connection`, `Executor`, `Prepared`, and `SqlWriter` traits. You can write a [custom driver](10-drivers.md) for another SQL or NoSQL store without touching `tank-core`.
- **Rich type mapping**: Built-in conversions for standard Rust types, decimals, dates, UUIDs, intervals, JSON, arrays, and maps, plus `AsValue` for custom types. Network drivers support TLS when configured in the connection URL.

## No-Fly Zone

Some features are deliberately out of scope:

- **No schema migrations**: Tank creates and drops tables for fast setup, tests, and prototypes, then stays out of the way of dedicated migration tools.
- **No implicit joins or nested entities**: Entities never hold other entities as fields. Joins are explicit via `join!`, so an innocent field access will never hide an N+1 query.

## Equipment
###### Core Arsenal
- [**tank**](https://crates.io/crates/tank): The command vehicle, main crate to use together with a driver.
- [**tank-core**](https://crates.io/crates/tank-core): All the heavy machinery that makes Tank move.
- [**tank-macros**](https://crates.io/crates/tank-macros): Derives and helper macros.
- [**tank-tests**](https://crates.io/crates/tank-tests): Shared integration tests for drivers.

###### Drivers
- [**tank-postgres**](https://crates.io/crates/tank-postgres)
- [**tank-sqlite**](https://crates.io/crates/tank-sqlite)
- [**tank-mysql**](https://crates.io/crates/tank-mysql)
- [**tank-duckdb**](https://crates.io/crates/tank-duckdb)
- [**tank-mongodb**](https://crates.io/crates/tank-mongodb)
- [**tank-scylladb**](https://crates.io/crates/tank-scylladb)
- [**tank-valkey**](https://crates.io/crates/tank-valkey)

All the crates in this workspace share the same version. See the [Capabilities Matrix](04-capabilities.md) for per-driver feature support and engine caveats.

## Support the Mission

Tank is an independent project maintained by [barsdeveloper](https://github.com/barsdeveloper). Building and testing drivers across several database families takes steady work, and sponsorship directly buys time for new drivers, bug fixes, and performance improvements.

If Tank is useful to you or your team, please consider [sponsoring via GitHub Sponsors](https://github.com/sponsors/TankHQ).

*Hold the line. Maintain discipline. Tank out.*
