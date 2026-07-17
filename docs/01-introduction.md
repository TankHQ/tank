# Introduction
###### *Field Manual Section 1* - Mission Briefing

Welcome to the Tank field manual. This is the quick guide for developers who want to drive, fight and survive with Tank (Table Abstraction & Navigation Kit): the Rust data layer.

In plain terms, Tank is a thin layer over your database workflow for Rust teams that need one interface across different databases. Whether you are digging into a local SQLite trench, coordinating a distributed ScyllaDB offensive, or managing a Postgres stronghold, Tank keeps your entity and query code consistent. You define your entities. Tank and your chosen driver handle the ballistics.

## Mission Objectives
Tank aims to provide a clean ORM design focused on ergonomics, flexibility and broad database support.

- **Async operations** - Non-blocking database operations built for async Rust.
- **Designed to be extensible** - Add or swap database drivers without changing Tank's core.
- **SQL and NoSQL support** - One Tank, all terrains.
- **Transaction abstraction** - Commit on success or rollback and retreat.
- **Rich type arsenal** - Automatic conversions between Rust and database types.
- **Optional appender API** - High caliber bulk inserts where the database supports them.
- **TLS** - No open radios for drivers that connect over the network.
- **Joins** - Explicit multi-unit coordination.
- **Raw SQL** - You're never limited by the abstractions provided.
- **Zero setup** - Skip training. Go straight to live fire.

## No-Fly Zone
- No schema migrations (only table creation and drop for fast setup).
- No implicit joins (no entities as fields, joins are explicit, every alliance is signed).

## Equipment
###### Core Arsenal
- [**tank**](https://crates.io/crates/tank): The command vehicle, main crate to use together with a driver.
- [**tank-core**](https://crates.io/crates/tank-core): All the heavy machinery that makes the Tank move.
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

All the crates in this workspace share the same version.

## Why Tank?
**Intelligence Report**: Existing Rust data layers are effective, but Tank takes a different approach: one entity definition, explicit queries and an extensible driver model for SQL and NoSQL databases.

**1. Modular Architecture**: Database support lives behind driver traits rather than a hardcoded list in the core library. A driver can be implemented for a new SQL or NoSQL database without changing Tank itself. If it can hold data, Tank can likely target it.

**2. Zero Boilerplate**: Field operations shouldn't require filling out forms in triplicate. Define a normal Rust struct once, then let Tank's macros derive table creation, selection and insertion. The result is less setup for tests and prototypes, with the same API available across production backends.

## Support the Mission
Building and maintaining drivers for several database families is a major effort. **If Tank saves your company time, infrastructure headaches, or boilerplate, please consider supporting its development.**

Sponsorship helps keep Tank maintained, well-tested and moving toward new capabilities faster.

🔗 **[Sponsor the Commander via GitHub Sponsors](https://github.com/sponsors/TankHQ)**. Tank is independently designed, written and maintained by [barsdeveloper](https://github.com/barsdeveloper).

*Hold the line. Maintain discipline. Tank out.*
