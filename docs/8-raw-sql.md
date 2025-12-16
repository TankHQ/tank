# Raw SQL
###### *Field Manual Section 8* - Precision Fire

Sometimes you need to drop the abstractions and put steel directly on target. Tank lets you fire raw SQL or multi‑statement batches (where supported) while still decoding rows into typed entities. This section covers building raw statements, executing mixed result streams, and converting rows back into your structs.

## Entry Points
Three firing modes:
- `executor.run(query)`: Streams a mix of `QueryResult::{Row, Affected}` for all statements contained in the query. Multiple statements in one batch are only available when the driver supports.
- `executor.fetch(query)`: Convenience method that yields only rows. Internally calls `Executor::run` and discards `QueryResult::Affected`.
- `executor.execute(query)`: Damage report only. Aggregates all `RowsAffected` across the batch and returns a single total. Internally calls `Executor::run` and discards rows (if any).

Anything implementing [`AsQuery`](https://docs.rs/tank/latest/tank/trait.AsQuery.html) works: `String`, `&str`, `Query<D>`, or `&mut Query<D>`.

## Composing SQL With `SqlWriter`
Every driver exposes a `SqlWriter` for dialect‑correct sql fragments. Concatenate multiple statements into one `String`. Writers add separators (`;`) automatically.

Example building 8 statements (1 *CREATE SCHEMA* included by the first *CREATE TABLE*, 2 *CREATE TABLE*, 3 *INSERT INTO* and 2 *SELECT*):
```rust
let writer = executor.driver().sql_writer();
let mut sql = String::new();
writer.write_create_table::<One>(&mut sql, true);
writer.write_create_table::<Two>(&mut sql, false);
writer.write_insert(&mut sql, &[One { string: "ddd".into() }, One { string: "ccc".into() }], false);
writer.write_insert(&mut sql, &[Two { a2: 21, string: "aaa".into() }, Two { a2: 22, string: "bbb".into() }], false);
writer.write_insert(&mut sql, &[One { a1: 11, string: "zzz".into(), c1: 512 }], false);
writer.write_select(&mut sql, [One::a1, One::string, One::c1], One::table(), &true, None);
writer.write_select(&mut sql, Two::columns(), Two::table(), &true, None);
// Fire the batch
let results = executor.run(sql).try_collect::<Vec<_>>().await?;
```

### Mixed Results
Each statement yields `Affected` or one or more `Row` values. Collect and filter as needed:
```rust
use tank::QueryResult;
let rows = results
    .into_iter()
    .filter_map(|r| match r { QueryResult::Row(row) => Some(row), _ => None })
    .collect::<Vec<_>>();
```

## Decoding Rows Into Entities
`QueryResult::Row` carries labeled columns. Any type with `#[derive(Entity)]` can be reconstructed using `Entity::from_row(row)` provided by the derive. Labels must match the field mapping (custom column names via `#[tank(name = "...")]` are respected). Missing columns use `Default` when available; otherwise an error is returned.
```rust
#[derive(Entity)]
struct Two { a2: u32, string: String }
// After collecting rows
let entity = Two::from_row(row)?; // Strongly typed reconstruction
```

You can interleave custom decoding for ad‑hoc structs defined inline, useful when projecting reduced column sets:
```rust
#[derive(Entity)]
struct Projection { callsign: String, strength: i8 }
let (callsign, strength) = Projection::from_row(row).map(|p| (p.callsign, p.strength))?;
```

## Prepared Statements
When the objective is fixed and only the parameters change, prepare the firing solution once, then reload with new values. Use `executor.prepare("...")` with parameter placeholders, bind values, and then fire again.

Using the operations schema (`Operator`, `RadioLog`) from Section 6:
```rust
let mut query = executor
    .prepare(
        indoc! {r#"
            SELECT message
            FROM operations.radio_log
            WHERE rssi > ? AND unit_callsign = ?
            LIMIT ?
        "#}
        .into(),
    )
    .await?;
query.bind(40_i32)?;
query.bind("Alpha-1")?;
query.bind(50_u32)?;

let messages: Vec<_> = executor
    .fetch(&mut query)
    .map_ok(|row| row.values[0].clone())
    .try_collect()
    .await?;
```

You can also build prepared statements via entities for common patterns:
```rust
use tank::{Query};
let mut query = RadioLog::prepare_find(
    executor,
    &expr!(RadioLog::signal_strength > ?),
    Some(50),
).await?;
if let Query::Prepared(p) = &mut query { p.bind(40)?; }
let messages: Vec<_> = executor
    .fetch(query)
    .map_ok(|row| row.values[0].clone())
    .try_collect()
    .await?;
```

Prepared statements cache driver parsing/optimizer state (when available) and validate parameter conversions at bind time.

### Notes & Driver Support
- `SqlWriter::write_create_table::<T>(&mut sql, include_schema)` will emit `CREATE SCHEMA` first when `include_schema` is `true` and the backend supports schemas.
- Streams returned by `executor.run(...)` are ordered by statement execution; interleave `Affected` and `Row` accordingly.
- When selecting a subset of columns, ensure the labels match the entity fields present. Missing labels require `Default` for omitted fields.
- Prepared queries use positional parameters across drivers.
- Reuse prepared queries across multiple executions: call `clear_bindings()` to reset parameters and `bind(...)` again before the next shot.

*Raw fire authorized. Execute with precision. Tank out.*
