# Introduction
###### *Field Manual Section 1* - Mission Briefing

Welcome to the Tank field manual. This is the quick-and-mean guide for developers who want to drive, fight and survive with Tank (Table Abstraction & Navigation Kit): the Rust data layer.

In plain terms: Tank is a lightweight, multi-database SQL toolkit. Define entities, run async queries, and mix raw SQL with ergonomic helpers without locking you into an all-or-nothing ORM worldview.

## Mission objectives
- **Async operations** - Fire and forget.
- **Designed to be extensible** - Swap databases like changing magazines mid-battle.
- **SQL and NoSQL support** - One Tank, all terrains.
- **Transactions abstraction** - Commit on success or rollback and retreat.
- **Rich type arsenal** - Automatic conversions between Rust and database types.
- **Optional appender API** - High caliber bulk inserts.
- **TLS** - No open radios on this battlefield.
- **Joins** - Multi unit coordination.
- **Raw SQL** - You're never limited by the abstractions provided.

## No-fly zone
- No schema migrations (just table creation and destroy for fast setup).
- No implicit joins (no entities as fields, joins are explicit, every alliance is signed).
- No complex query builder (write raw SQL and take full credit).

## Equipment
###### Core Arsenal
- [**tank**](https://crates.io/crates/tank): The command vehicle, main crate to use together with a driver.
- [**tank-core**](https://crates.io/crates/tank-core): All the heavy machinery that makes the Tank move.
- [**tank-macros**](https://crates.io/crates/tank-macros): Derives and helper macros.
- [**tank-tests**](https://crates.io/crates/tank-tests): Shared integration tests for drivers.

###### Drivers
- [**tank-duckdb**](https://crates.io/crates/tank-duckdb): DuckDB.
- [**tank-sqlite**](https://crates.io/crates/tank-sqlite): SQLite.
- [**tank-postgres**](https://crates.io/crates/tank-postgres): Postgres.
- [**tank-mysql**](https://crates.io/crates/tank-mysql): MySQL and MariaDB.

All the crates in this workspace share the same version.

## Why Tank?
Tank is a thin, battle-ready layer over your database workflow.
It keeps the learning curve low, just a handful of clear concepts. It stays deliberately lean and essential, few moving parts, fast maneuvers.

Because its scope is tight, Tank can deploy on many fronts, from classic SQL databases to non-SQL theaters. At the same time, it doesn't limit your capabilities: Tank never tries to hide SQL behind a heavy query builder: you can write plain SQL whenever you need and still benefit from its rich type-conversion features. In this way, it's similar in spirit to [SQLx](https://crates.io/crates/sqlx) but unlike SQLx, Tank does not perform compile-time SQL validation. It prioritizes runtime flexibility and multi-database support over static checking.

Tank also provides convenient methods to set up tables and get database communication running in just a few lines of code, all through a unified API that works the same regardless of the backend. Perfect for spinning up tests and prototypes rapidly while still scaling to production backends.

*Hold the line. Maintain discipline. Tank out.*
