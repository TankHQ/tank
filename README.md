<div align="center">
    <img width="300" height="300" src="docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# Tank
Tank (Table Abstraction & Navigation Kit): the Rust data layer.

Simple and flexible ORM that manages in a unified way data from different sources.

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

**Known battlefields**:
- DuckDB
- SQLite
- PostgreSQL
- MySQL / MariaDB
- Cassandra / ScyllaDB
- MongoDB WIP
- More to be decided...

## Mission objectives
Tank exists to implement the best possible design for a ORM written in Rust. A a clean-slate design focused on ergonomics, flexibility, licensing and broad database support.

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

## Getting started
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
use tank::{Entity, Executor, Result};

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
use std::{borrow::Cow, collections::HashSet, sync::LazyLock};
use tank::{Entity, Executor, Result, expr, stream::TryStreamExt};
use tank_duckdb::DuckDBDriver;

async fn data() -> Result<()> {
    let driver = DuckDBDriver::new();
    let connection = driver
        .connect("duckdb://../target/debug/tests.duckdb?mode=rw".into())
        .await?;

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
    Tank::create_table(connection, true, true).await?;

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
    my_tank.save(connection).await?;

    /*
     * DuckDB uses the appender API. Other drivers generate a INSERT:
     * INSERT INTO "army"."tank" ("name", "country", "caliber", "speed", "is_operational", "units_produced") VALUES
     *     ('T-34/85', 'Soviet Union', 85, 53.0, false, 49200),
     *     ('M1 Abrams', 'USA', 120, 72.0, true, NULL);
     */
    Tank::insert_many(
        connection,
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
    let tanks = Tank::find_many(
        connection,
        expr!(Tank::is_operational == false),
        Some(1000),
    )
    .try_collect::<Vec<_>>()
    .await?;

    assert_eq!(
        tanks
            .iter()
            .map(|t| t.name.to_string())
            .collect::<HashSet<_>>(),
        HashSet::from_iter(["Tiger I".into(), "T-34/85".into()])
    );
    Ok(())
}
```

*Rustaceans don't hide behind ORMs, they drive Tanks.*
