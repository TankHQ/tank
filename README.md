<div align="center">
    <img width="300" height="300" src="docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# Tank
Tank (Table Abstraction & Navigation Kit) is the Rust data layer that lets you define an entity once and use the same explicit query API across a wide range of database management systems.

**One entity model. Any terrain.**

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank

## Mission Briefing
In plain terms, Tank is a thin layer over your database workflow for Rust teams that need one interface across different databases. Whether you are digging into a local SQLite trench, coordinating a distributed ScyllaDB offensive, or managing a Postgres stronghold, Tank keeps your entity and query code consistent. You define your entities. Tank and your chosen driver handle the ballistics.

**Known battlefields**:
- Postgres
- SQLite
- MySQL/MariaDB
- DuckDB
- MongoDB
- ScyllaDB/Cassandra
- Valkey/Redis

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

## Why Tank?
**Intelligence Report**: Existing Rust data layers are effective, but Tank takes a different approach: one entity definition, explicit queries and an extensible driver model for SQL and NoSQL databases.

**1. Modular Architecture**: Database support lives behind driver traits rather than a hardcoded list in the core library. A driver can be implemented for a new SQL or NoSQL database without changing Tank itself. If it can hold data, Tank can likely target it.

**2. Zero Boilerplate**: Field operations shouldn't require filling out forms in triplicate. Define a normal Rust struct once, then let Tank's macros derive table creation, selection and insertion. The result is less setup for tests and prototypes, with the same API available across production backends.

## Operational Guide

*For more examples, check the [cheat sheet](https://tankhq.github.io/tank/00-cheat-sheet.html).*

1) Arm your cargo
```sh
cargo add tank
```

2) Choose your battlefield
```sh
cargo add tank-duckdb
```

3) Define unit schematics
```rust
use std::borrow::Cow;
use tank::{Entity, Result};

#[derive(Entity)]
#[tank(schema = "army")]
pub struct Tank {
    #[tank(primary_key)]
    pub name: String,
    pub country: Cow<'static, str>,
    #[tank(name = "caliber")]
    pub caliber_mm: u16,
    #[tank(name = "speed")]
    pub speed_kmh: f32,
    pub is_operational: bool,
    pub units_produced: Option<u32>,
}
```

4) Fire for effect
```rust
use std::collections::HashSet;
use tank::{ConnectionPool, Driver, PoolConfig, expr, stream::TryStreamExt};
use tank_duckdb::DuckDBDriver;

async fn data() -> Result<()> {
    let driver = DuckDBDriver::new();
    let pool = driver
        .connect_pool(
            "duckdb://../target/debug/tests.duckdb?mode=rw".into(),
            PoolConfig::new(),
        )
        .await?;
    let mut connection = pool.get().await?;

    let my_tank = Tank {
        name: "Tiger I".into(),
        country: "Germany".into(),
        caliber_mm: 88,
        speed_kmh: 45.4,
        is_operational: false,
        units_produced: Some(1_347),
    };

    /*
     * CREATE SCHEMA IF NOT EXISTS "army";
     * CREATE TABLE IF NOT EXISTS "army"."tank" (
     *     "name" VARCHAR PRIMARY KEY,
     *     "country" VARCHAR NOT NULL,
     *     "caliber" USMALLINT NOT NULL,
     *     "speed" FLOAT NOT NULL,
     *     "is_operational" BOOLEAN NOT NULL,
     *     "units_produced" UINTEGER);
     */
    Tank::create_table(&mut connection, true, true).await?;

    /*
     * INSERT INTO "army"."tank" ("name", "country", "caliber", "speed", "is_operational", "units_produced") VALUES
     *     ('Tiger I', 'Germany', 88, 45.4, false, 1347)
     * ON CONFLICT ("name") DO UPDATE SET
     *     "country" = EXCLUDED."country",
     *     "caliber" = EXCLUDED."caliber",
     *     "speed" = EXCLUDED."speed",
     *     "is_operational" = EXCLUDED."is_operational",
     *     "units_produced" = EXCLUDED."units_produced";
     */
    my_tank.save(&mut connection).await?;

    /*
     * DuckDB uses the appender API. Other drivers generate an INSERT:
     * INSERT INTO "army"."tank" ("name", "country", "caliber", "speed", "is_operational", "units_produced") VALUES
     *     ('T-34/85', 'Soviet Union', 85, 53.0, false, 49200),
     *     ('M1 Abrams', 'USA', 120, 72.0, true, NULL);
     */
    Tank::insert_many(
        &mut connection,
        &[
            Tank {
                name: "T-34/85".into(),
                country: "Soviet Union".into(),
                caliber_mm: 85,
                speed_kmh: 53.0,
                is_operational: false,
                units_produced: Some(49_200),
            },
            Tank {
                name: "M1 Abrams".into(),
                country: "USA".into(),
                caliber_mm: 120,
                speed_kmh: 72.0,
                is_operational: true,
                units_produced: None,
            },
        ],
    )
    .await?;

    /*
     * SELECT "name", "country", "caliber", "speed", "is_operational", "units_produced"
     * FROM "army"."tank"
     * WHERE "is_operational" = false
     * LIMIT 1000;
     */
    let tanks = Tank::find_many(&mut connection, expr!(Tank::is_operational == false), Some(1000))
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(
        tanks
            .iter()
            .map(|t| t.name.to_string())
            .collect::<HashSet<_>>(),
        HashSet::from_iter(["Tiger I".into(), "T-34/85".into()])
    );
    println!("Tank is operational: {} units found.", tanks.len());
    Tank::drop_table(&mut connection, true, true).await?;
    Ok(())
}
```

## Support the Mission
Building and maintaining drivers for several database families is a major effort. **If Tank saves your company time, infrastructure headaches, or boilerplate, please consider supporting its development.**

Sponsorship helps keep Tank maintained, well-tested and moving toward new capabilities faster.

🔗 **[Sponsor the Commander via GitHub Sponsors](https://github.com/sponsors/TankHQ)**. Tank is independently designed, written and maintained by [barsdeveloper](https://github.com/barsdeveloper).

*Rustaceans don't hide behind ORMs, they drive Tanks.*
