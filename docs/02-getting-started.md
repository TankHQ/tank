# Getting Started
###### *Field Manual Section 2* - Deployment Orders

What follows is just a visit to the shooting range, not the full campaign. This minimal example shows Tank in action: connecting, defining a unit and executing basic maneuvers.

**Mission brief**: install Tank and a driver, define an entity, create the table, insert a few rows, then fire a query. For full tactical exercises including transactions, complex queries and multi-driver deployments, proceed to the [*Field Manual Section 3* - Supply Lines](03-connection.md).

> [!TIP]
> Tank exposes an async API. Run these examples under an async runtime like [Tokio](https://crates.io/crates/tokio) and `.await` your operations. Embedded drivers may still perform synchronous native work.

1) Arm your cargo
```sh
cargo add tank
cargo add tokio --features macros,rt-multi-thread
```

2) Choose your battlefield

Check the [drivers list](01-introduction.md#drivers) and select one that matches your terrain.
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
            "duckdb://tests.duckdb?mode=rw".into(),
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
     *     "name" TEXT PRIMARY KEY,
     *     "country" TEXT NOT NULL,
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
     * DuckDB uses the appender API. Equivalent SQL:
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
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    data().await
}
```

Place both Rust snippets in `src/main.rs` and run `cargo run`. Tank will create `tests.duckdb` in the current directory and report `Tank is operational: 2 units found.`

> [!NOTE]
> The `expr!` macro is explained in the [Field Manual Section 8 - Tactical Coordination](08-advanced-operations.md#expr).

*Rustaceans don't hide behind ORMs, they drive Tanks.*
