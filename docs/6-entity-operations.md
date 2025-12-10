# Entity Operations
###### *Field Manual Section 6* - Front-Line Extraction

The Entity is your combat unit, a Rust struct mapped one-to-one with a database table. This section trains you on the basic maneuvers every unit must master: insertions, deletions, and extractions.

## Mission Scope
Every tactical primitive you can execute against an `Entity`. Each item maps to a single, unambiguous action. Almost all higher-level patterns are just compositions of these fundamentals.
* [`Entity::create_table()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.create_table): establish operating base
* [`Entity::drop_table()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.drop_table): break camp
* [`Entity::insert_one()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.insert_one): deploy a single unit
* [`Entity::insert_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.insert_many): bulk deployment
* [`Entity::find_pk()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.find_pk): identify the target
* [`Entity::find_one()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.find_one): silent recon
* [`Entity::find_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.find_many): wide-area sweep
* [`Entity::delete_one()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.delete_one): precision strike
* [`Entity::delete_many()`](https://docs.rs/tank/latest/tank/trait.Entity.html#tymethod.delete_many): scorched-earth withdrawal
* [`entity.save()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.save): resupply and hold the position
* [`entity.delete()`](https://docs.rs/tank/latest/tank/trait.Entity.html#method.delete): stand-down order

## Operations Schema
This is the schema we will use for every operation example that follows. All CRUD, streaming, prepared, and batching demonstrations below act on these two tables so you can focus on behavior instead of switching contexts. `Operator` is the identity table, `RadioLog` references an operator (foreign key) to record transmissions.
::: code-group
```rust [Rust]
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
```
```sql [SQL]
CREATE TABLE IF NOT EXISTS operations.radio_operator (
    id UUID PRIMARY KEY,
    callsign VARCHAR NOT NULL,
    rank VARCHAR NOT NULL,
    enlistment_date DATE NOT NULL,
    is_certified BOOLEAN NOT NULL);

CREATE TABLE IF NOT EXISTS operations.radio_log (
    id UUID PRIMARY KEY,
    operator UUID NOT NULL REFERENCES operations.radio_operator(id),
    message VARCHAR NOT NULL,
    unit_callsign VARCHAR NOT NULL,
    tx_time TIMESTAMP WITH TIME ZONE NOT NULL,
    rssi TINYINT NOT NULL);
```
:::

## Setup
Deployment is initial insertion of your units into the theater: create tables (and schema) before any data flows, tear them down when the operation ends.
```rust
RadioLog::drop_table(executor, true, false).await?;
Operator::drop_table(executor, true, false).await?;

Operator::create_table(executor, false, true).await?;
RadioLog::create_table(executor, false, false).await?;
```

Key points:
- `if_not_exists` / `if_exists` guard repeated ops.
- Schema creation runs before the table when requested.
- Foreign key in `RadioLog.operator` enforces referential discipline.

## Insert
Single unit insertion:
```rust
Operator {
    id: operator_1_id,
    callsign: "SteelHammer".into(),
    service_rank: "Major".into(),
    enlisted: date!(2015 - 06 - 20),
    is_certified: true,
},
Operator::insert_one(executor, &operator).await?;
```

Bulk deployment of logs:
```rust
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
```

## Find
Find by primary key:
```rust
let found = Operator::find_pk(&mut executor, &operator.primary_key()).await?;
if let Some(op) = found { /* confirm identity */ }
```

First matching row (use a predicate with `find_one`):
```rust
if let Some(radio_log) =
    RadioLog::find_one(executor, &expr!(RadioLog::unit_callsign == "Alpha-1")).await?
{
    log::debug!("Found radio log: {:?}", radio_log.id);
}
```

Under the hood: `find_one` is just `find_many` with a limit of 1.

All matching transmissions with limit:
```rust
{
    let mut stream = pin!(RadioLog::find_many(
        executor,
        &expr!(RadioLog::signal_strength >= 40),
        Some(100)
    ));
    while let Some(radio_log) = stream.try_next().await? {
        log::debug!("Found radio log: {:?}", radio_log.id);
    }
    // Executor is released from the stream at the end of the scope
}
```
The stream must be pinned with [`std::pin::pin`](https://doc.rust-lang.org/std/pin/macro.pin.html) so the async machinery can safely borrow it without relocation mid‑flight.

## Save
`save()` attempts insert or update (UPSERT) if the driver supports conflict clauses. Otherwise it falls back to an insert and may error if the row already exists.
```rust
let mut operator = operator;
operator.callsign = "SteelHammerX".into();
operator.save(executor).await?;
```

RadioLog also has a primary key, so editing a message:
```rust
let mut log = RadioLog::find_one(executor, &expr!(RadioLog::message == "Ping #2"))
    .await?
    .expect("Missing log");
log.message = "Ping #2 ACK".into();
log.save(executor).await?;
```

If a table has no primary key, `save()` returns an error, use `insert_one` instead.

## Delete
Precision strike:
```rust
RadioLog::delete_one(executor, log.primary_key()).await?;
```

Scorched earth pattern:
```rust
RadioLog::delete_many(executor, &expr!(RadioLog::operator == #operator_id)).await?;
```

Instance form (validates exactly one row):
```rust
operator.delete(executor).await?;
```

## Prepared
Filter transmissions above a strength threshold:
```rust
let mut query =
    RadioLog::prepare_find(executor, &expr!(RadioLog::signal_strength > ?), None).await?;
p.bind(40)?

let messages: Vec<_> = executor
    .fetch_many(query)
    .map_ok(|row| row.values[0].clone())
    .try_collect()
    .await?;
```

## Multi-Statement
Combine delete + insert + select in one roundtrip:
```rust
let writer = executor.driver().sql_writer();
let mut sql = String::new();
writer.write_delete::<RadioLog>(&mut sql, &expr!(RadioLog::signal_strength < 10));
writer.write_insert(&mut sql, [&operator], false);
writer.write_insert(
    &mut sql,
    [&RadioLog {
        id: Uuid::new_v4(),
        operator: operator.id,
        message: "Status report".into(),
        unit_callsign: "Alpha-1".into(),
        transmission_time: OffsetDateTime::now_utc(),
        signal_strength: 55,
    }],
    false,
);
writer.write_select(
    &mut sql,
    RadioLog::columns(),
    RadioLog::table(),
    &expr!(true),
    Some(50),
);
{
    let mut stream = pin!(executor.run(sql));
    while let Some(result) = stream.try_next().await? {
        match result {
            QueryResult::Row(row) => log::debug!("Row: {row:?}"),
            QueryResult::Affected(RowsAffected { rows_affected, .. }) => {
                log::debug!("Affected rows: {rows_affected:?}")
            }
        }
    }
}
```
While the returned stream is in scope the executor is locked to it and cannot service other maneuvers, contain the pinned stream in a tight block so dropping it releases the executor promptly.

Process `QueryResult::Affected` then `QueryResult::Row` items sequentially.

## Error Signals & Edge Cases
- `save()` / `delete()` on entities without PK result in immediate error.
- `delete()` with affected rows not exactly one results in error.
- Prepared binds validate conversion, failure returns `Result::Err`.

## Performance Hints (Radio Theater)
- Use prepared statements for hot paths (changing only parameters).
- Limit streaming scans with a numeric `limit` to avoid unbounded pulls.

*Targets locked. Orders executed. Tank out.*
