# Introduction
###### *Field Manual Section 1* - Mission Briefing

Welcome to the Tank field manual. This is the quick-and-mean guide for developers who want to drive, fight and survive with Tank (Table Abstraction & Navigation Kit): the Rust data layer.

In plain terms: Tank is a thin, battle-ready layer over your database workflow, designed for the Rust operator who needs to deploy across multiple environments without changing the kit.

It doesn't matter if you are digging into a local SQLite trench, coordinating a distributed ScyllaDB offensive, or managing a Postgres stronghold, Tank provides a unified interface. You define your entities once. Tank handles the ballistics.

## Mission objectives
Tank exists to implement the best possible design for a ORM written in Rust. A a clean-slate design focused on ergonomics, flexibility and broad database support.

- **Async operations** - Fire and forget.
- **Designed to be extensible** - Swap databases like changing magazines mid-battle.
- **SQL and NoSQL support** - One Tank, all terrains.
- **Transactions abstraction** - Commit on success or rollback and retreat.
- **Rich type arsenal** - Automatic conversions between Rust and database types.
- **Optional appender API** - High caliber bulk inserts.
- **TLS** - No open radios on this battlefield.
- **Joins** - Multi unit coordination.
- **Raw SQL** - You're never limited by the abstractions provided.
- **Zero setup** - Skip training. Go straight to live fire.

## No-fly zone
- No schema migrations (just table creation and destroy for fast setup).
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

All the crates in this workspace share the same version.

## Why Tank?
**Intelligence Report**
A quick recon of the battlefield revealed that while existing heavy weaponry is effective, there was a critical need for a more adaptable, cleaner design capable of true multi-theater dominance. Tank was designed from scratch to address these weaknesses.

**1. Modular Architecture**
Some systems rely on hardcoded enums for database support, which limits flexibility. If a backend isn't in the core list, it cannot be used. Tank uses a extensible design pattern. A driver can be implemented for *any* database (SQL or NoSQL) without touching the core library. If it can hold data, Tank can likely target it.

**2. Zero Boilerplate**
Field operations shouldn't require filling out forms in triplicate. Some tools force data definition twice: once in a complex DSL and again as a Rust struct. Tank cuts the red tape. **One struct. One definition.** The macros handle table creation, selection, and insertion automatically based on standard Rust structs. You can set up tables and get database communication running in just a few lines of code, all through a unified API that works the same regardless of the backend. Perfect for spinning up tests and prototypes rapidly while still scaling to production backends.

*Hold the line. Maintain discipline. Tank out.*
