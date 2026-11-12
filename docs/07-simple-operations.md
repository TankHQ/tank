# Simple Operations
###### *Field Manual Section 7* - Front-Line Extraction

An entity maps one-to-one with a database table. This section trains you on the basic maneuvers every unit must master: insertions, deletions, and extractions.

## Available Operations
Core operations on `Entity`:
* [`Entity::create_table()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.create_table): create table and optionally schema
* [`Entity::drop_table()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.drop_table): drop table and optionally schema
* [`Entity::insert_one()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.insert_one): insert one row
* [`Entity::insert_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.insert_many): insert many rows (possibly in an optimized way)
* [`Entity::prepare_find()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.prepare_find): prepare a SELECT query against this table
* [`Entity::find_one()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.find_one): first matching row
* [`Entity::find_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.find_many): stream matching entities
* [`Entity::delete_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.delete_many): delete by condition
* [`entity.save()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.save): insert or update (works only for entities with a primary key)
* [`entity.delete()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.delete): delete this entity (works only for entities with a primary key)

## Operations Schema
This is the schema we will use for every operation example that follows. All CRUD, streaming, prepared, and batching demonstrations below act on these two tables so you can focus on behavior instead of switching contexts. `Operator` is the identity table, `RadioLog` references an operator (foreign key) to record transmissions.

The SQL below uses PostgreSQL syntax. Other drivers choose their own column types.
::: code-group
```rust [Rust]
use std::pin::pin;
use tank::{
    Driver, DynQuery, Entity, Sql, QueryResult, RowsAffected, SqlWriter, anyhow, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, OffsetDateTime, macros::date};
use uuid::Uuid;

<!--@include: @/../tank-tests/src/operations.rs#operations-schema-->
```
```sql [SQL]
CREATE TABLE IF NOT EXISTS operations.radio_operator (
    id UUID PRIMARY KEY,
    callsign TEXT NOT NULL,
    rank TEXT NOT NULL,
    enlistment_date DATE NOT NULL,
    is_certified BOOLEAN NOT NULL);

CREATE TABLE IF NOT EXISTS operations.radio_log (
    id UUID PRIMARY KEY,
    operator UUID NOT NULL REFERENCES operations.radio_operator(id),
    message TEXT NOT NULL,
    unit_callsign TEXT NOT NULL,
    tx_time TIMESTAMP WITH TIME ZONE NOT NULL,
    rssi SMALLINT NOT NULL);
```
:::

## Setup
Create/drop tables (and schema) as needed:
```rust
RadioLog::drop_table(executor, true, false).await?;
Operator::drop_table(executor, true, false).await?;

Operator::create_table(executor, false, true).await?;
RadioLog::create_table(executor, false, false).await?;
```

Key points:
- `if_not_exists`/`if_exists` guard repeated ops.
- Schema creation runs before the table when requested.
- `RadioLog` references an operator (foreign key) to record transmissions.

## Insert
Single unit insertion:
```rust
let operator = Operator {
    id: Uuid::new_v4(),
    callsign: "SteelHammer".into(),
    service_rank: "Major".into(),
    enlisted: date!(2015 - 06 - 20),
    is_certified: true,
};
Operator::insert_one(executor, &operator).await?;
```

Insert many:

<<< @/../tank-tests/src/operations.rs#operations-insert-many

## Find
First matching row (use a predicate):
```rust
if let Some(radio_log) =
    RadioLog::find_one(executor, expr!(RadioLog::unit_callsign == "Alpha-1")).await?
{
    log::debug!("Found radio log: {:?}", radio_log.id);
}
```

Under the hood: `find_one` is just `find_many` with a limit of 1.

Stream matching rows with a limit:

<<< @/../tank-tests/src/operations.rs#operations-stream

The stream must be pinned with [`std::pin::pin`](https://doc.rust-lang.org/std/pin/macro.pin.html) so the async machinery can safely borrow it without relocation mid-flight.

## Save
`save()` inserts or updates (UPSERT) if supported. Otherwise it falls back to an insert and may error if the row already exists.
```rust
let mut operator = operator;
operator.callsign = "SteelHammerX".into();
operator.save(executor).await?;
```

Instance method to save the current entity (works only for entities defining a primary key):
```rust
let mut log = RadioLog::find_one(executor, expr!(RadioLog::message == "Ping #2"))
    .await?
    .ok_or_else(|| anyhow!("Missing log"))?;
log.message = "Ping #2 ACK".into();
log.save(executor).await?;
```

If a table has no primary key, `save()` returns an error, use `insert_one` instead.

## Prepared
Filter by strength (prepared):

<<< @/../tank-tests/src/operations.rs#operations-prepared

## Delete
Delete many entities matching an expression:
```rust
let operator_id = operator.id;
RadioLog::delete_many(executor, expr!(RadioLog::operator == #operator_id)).await?;
```

Instance method to delete the current entity (works only for entities defining a primary key):
```rust
operator.delete(executor).await?;
```

## Multi-Statement
Delete + insert + select in one roundtrip:

<<< @/../tank-tests/src/operations.rs#operations-batch

While the stream is alive, it borrows the executor, which cannot service other queries. Enclose the pinned stream in a scope so both it and the borrow end before the next operation.

## Errors & Edge Cases
- `save()`/`delete()` on entities without PK result in immediate error.
- `delete()` errors when the driver reports an affected-row count other than one. Unknown counts are accepted.
- Parameter conversion failures return errors during binding or execution, depending on the driver.

## Performance
- Use prepared statements for hot paths (changing only parameters).
- Limit streaming scans with a numeric `limit` to avoid unbounded pulls.

*Targets locked. Orders executed. Tank out.*
