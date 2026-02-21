<div align="center">
    <img width="300" height="300" src="docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# Tank
Tank (Table Abstraction & Navigation Kit): the Rust data layer.

**One platform. Any terrain.**

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

## Mission Briefing
Tank is a thin, battle-ready layer over your database workflow, designed for the Rust operator who needs to deploy across multiple environments without changing the kit.

It doesn't matter if you are digging into a local SQLite trench, coordinating a distributed ScyllaDB offensive, or managing a Postgres stronghold, Tank provides a unified interface. You define your entities once. Tank handles the ballistics.

**Known battlefields**:
- Postgres
- SQLite
- MySQL / MariaDB
- DuckDB
- MongoDB
- Cassandra / ScyllaDB

## Mission Objectives
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

## Why Tank?
**Intelligence Report**
A quick recon of the battlefield revealed that while existing heavy weaponry is effective, there was a critical need for a more adaptable, cleaner design capable of true multi-theater dominance. Tank was designed from scratch to address these weaknesses.

**1. Modular Architecture**
Some systems rely on hardcoded enums for database support, which limits flexibility. If a backend isn't in the core list, it cannot be used. Tank uses a extensible design pattern. A driver can be implemented for *any* database (SQL or NoSQL) without touching the core library. If it can hold data, Tank can likely target it.

**2. Zero Boilerplate**
Field operations shouldn't require filling out forms in triplicate. Some tools force data definition twice: once in a complex DSL and again as a Rust struct. Tank cuts the red tape. **One struct. One definition.** The macros handle table creation, selection, and insertion automatically based on standard Rust structs. You can set up tables and get database communication running in just a few lines of code, all through a unified API that works the same regardless of the backend. Perfect for spinning up tests and prototypes rapidly while still scaling to production backends.

## Operational Guide
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
     * DuckDB uses the appender API. Other drivers generate an INSERT:
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
    let tanks = Tank::find_many(connection, expr!(Tank::is_operational == false), Some(1000))
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

## Examples

### Books
```rust
#[derive(Entity, Clone, PartialEq, Debug)]
#[tank(schema = "testing", name = "authors")]
pub struct Author {
    #[tank(primary_key, name = "author_id")]
    pub id: Passive<Uuid>,
    pub name: String,
    ...
}
#[derive(Entity, Clone, PartialEq, Debug)]
#[tank(schema = "testing", name = "books", primary_key = (Self::title, Self::author))]
pub struct Book {
    #[tank(column_type = (mysql = "VARCHAR(255)"))]
    pub title: String,
    #[tank(references = Author::id)]
    pub author: Uuid,
    ...
}
#[derive(Entity, PartialEq, Debug)]
struct BookAuthorResult {
    #[tank(name = "title")]
    book: String,
    #[tank(name = "name")]
    author: String,
}
let result = executor
    .fetch(
        QueryBuilder::new()
            .select(cols!(B.title, A.name))
            .from(join!(Book B JOIN Author A ON B.author == A.author_id))
            .where_expr(expr!(B.year < 2000))
            .order_by(cols!(B.title DESC))
            .build(&executor.driver()),
    )
    .map_ok(BookAuthorResult::from_row)
    .map(Result::flatten)
    .try_collect::<Vec<_>>()
    .await
    .expect("Failed to query books and authors joined");
assert_eq!(
    result,
    [
        BookAuthorResult{
            book: "The Hobbit".into(),
            author: "J.R.R. Tolkien".into()
        },
        BookAuthorResult{
            book: "Harry Potter and the Philosopher's Stone".into(),
            author: "J.K. Rowling".into(),
        },
    ]
);
```

### Radio Logs
```rust
#[derive(Entity)]
#[tank(schema = "operations", name = "radio_operator")]
pub struct Operator {
    #[tank(primary_key)]
    pub id: Uuid,
    pub callsign: String,
    #[tank(name = "rank")]
    pub service_rank: String,
    #[tank(name = "enlistment_date")]
    pub enlisted: Date,
    pub is_certified: bool,
}
#[derive(Entity)]
#[tank(schema = "operations")]
pub struct RadioLog {
    #[tank(primary_key)]
    pub id: Uuid,
    #[tank(references = Operator::id)]
    pub operator: Uuid,
    pub message: String,
    pub unit_callsign: String,
    #[tank(name = "tx_time")]
    pub transmission_time: OffsetDateTime,
    #[tank(name = "rssi")]
    pub signal_strength: i8,
}
let operator = Operator {
    id: Uuid::parse_str("21c90df5-00db-4062-9f5a-bcfa2e759e78").unwrap(),
    callsign: "SteelHammer".into(),
    service_rank: "Major".into(),
    enlisted: date!(2015 - 06 - 20),
    is_certified: true,
};
Operator::insert_one(executor, &operator).await?;
let op_id = operator.id;
let logs: Vec<RadioLog> = (0..5)
    .map(|i| RadioLog {
        id: Uuid::new_v4(),
        operator: op_id,
        message: format!("Ping #{i}"),
        unit_callsign: "Alpha-1".into(),
        transmission_time: OffsetDateTime::now_utc(),
        signal_strength: 42,
    })
    .collect();
RadioLog::insert_many(executor, &logs).await?;
if let Some(radio_log) = RadioLog::find_one(
    executor,
    expr!(RadioLog::unit_callsign == "Alpha-%" as LIKE),
)
.await?
{
    log::debug!("Found radio log: {:?}", radio_log.id);
}
let mut query =
    RadioLog::prepare_find(executor, expr!(RadioLog::signal_strength > ?), None).await?;
query.bind(40)?;
let _messages: Vec<_> = executor
    .fetch(query)
    .map_ok(|row| row.values[0].clone())
    .try_collect()
    .await?;
```

*Rustaceans don't hide behind ORMs, they drive Tanks.*
