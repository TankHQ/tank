# Raw SQL
###### *Field Manual Section 9* - Precision Fire

Sometimes you need to drop the abstractions and put steel directly on target. Tank lets you fire raw SQL or multi-statement batches (where supported) while still decoding rows into typed entities. This section covers building raw statements, executing mixed result streams, and converting rows back into your structs.

## Execution Methods
Three firing modes:
- `executor.run(query)`: Streams a mix of `QueryResult::{Row, Affected}` for all statements contained in the query. Multiple statements in one batch are only available when the driver supports them.
- `executor.fetch(query)`: Yields only rows. The default implementation calls `Executor::run` and discards `QueryResult::Affected`. Drivers may override it.
- `executor.execute(query)`: Damage report only. Aggregates all `RowsAffected` across the batch and returns a single total. Internally calls `Executor::run` and discards rows (if any).

Anything implementing [`AsQuery`](https://docs.rs/tank/latest/tank/trait.AsQuery.html) works: `String`, `&str`, `DynQuery`, `Query<D>`, or `&mut Query<D>`.

## Composing SQL With `SqlWriter`
SQL drivers expose a `SqlWriter` for dialect-correct SQL fragments. Use the writer to buffer multiple statements into one `DynQuery`. MongoDB and Valkey use native query representations and do not accept raw SQL.

`DynQuery` is the raw-query staging area. Use it while composing executable SQL text. Once you call `prepare(...)`, execution moves to a driver-specific prepared handle rather than an extendable SQL buffer.

Example building 9 statements (3 *CREATE TABLE*, 3 *INSERT INTO* and 3 *SELECT*):
```rust
use tank::{Driver, DynQuery, Entity, Sql, SqlWriter, stream::TryStreamExt};

#[derive(Entity)]
struct One { a1: u32, string: String, c1: u64 }
#[derive(Entity)]
struct Two { a2: u32, string: String }
#[derive(Entity)]
struct Three { string: String }

let writer = executor.driver().sql_writer();
let mut query = DynQuery::default();
writer.write_create_table(&mut query, One::table(), true);
writer.write_create_table(&mut query, Two::table(), true);
writer.write_create_table(&mut query, Three::table(), true);
writer.write_insert(&mut query, Sql().insert_into(Two::table()).values(&[Two { a2: 21, string: "aaa".into() }, Two { a2: 22, string: "bbb".into() }, Two { a2: 23, string: "eee".into() }]));
writer.write_insert(&mut query, Sql().insert_into(Three::table()).values(&[Three { string: "ddd".into() }, Three { string: "ccc".into() }]));
writer.write_select(
    &mut query,
    Sql()
        .select([Three::string])
        .from(Three::table())
        .where_expr(true),
);
writer.write_insert(&mut query, Sql().insert_into(One::table()).values(&[One { a1: 11, string: "zzz".into(), c1: 512 }]));
writer.write_select(
    &mut query,
    Sql()
        .select([One::a1, One::string, One::c1])
        .from(One::table())
        .where_expr(true),
);
writer.write_select(
    &mut query,
    Sql()
        .select([Two::a2, Two::string])
        .from(Two::table())
        .where_expr(true),
);
// Fire the batch
let results = executor.run(query).try_collect::<Vec<_>>().await?;
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
`QueryResult::Row` carries labeled columns. Any type with `#[derive(Entity)]` can be reconstructed using `Entity::from_row(row)` provided by the derive. Labels must match the field mapping (custom column names via `#[tank(name = "...")]` are respected). Missing columns retain the entity's `Default` values when it implements that trait; otherwise an error is returned.
```rust
#[derive(Entity)]
struct Two { a2: u32, string: String }
// After collecting rows
let entity = Two::from_row(row)?; // Strongly typed reconstruction
```

You can interleave custom decoding for ad-hoc structs defined inline, useful when projecting reduced column sets:
```rust
#[derive(Entity)]
struct Projection { callsign: String, strength: i8 }
let (callsign, strength) = Projection::from_row(row).map(|p| (p.callsign, p.strength))?;
```

## Prepared Statements
When the objective is fixed and only the parameters change, prepare the firing solution once, then reload with new values. Use `executor.prepare("...")` with parameter placeholders, bind values, and then fire again. Bind untrusted values rather than concatenating them into SQL.

Using the operations schema (`Operator`, `RadioLog`) from Section 7, with PostgreSQL's `$n` placeholders. DuckDB and MySQL use `?`; raw SQL is not rewritten into another dialect.
```rust
use tank::{Entity, stream::{StreamExt, TryStreamExt}};

#[derive(Entity)]
struct Message { message: String }

let mut query = executor
    .prepare(
        r#"
            SELECT message
            FROM operations.radio_log
            WHERE rssi > $1 AND unit_callsign = $2
            LIMIT $3
        "#,
    )
    .await?;
query.bind(40_i32)?;
query.bind("Alpha-1")?;
query.bind(50_u32)?;

let messages: Vec<_> = executor
    .fetch(&mut query)
    .map(|row| row.and_then(Message::from_row).map(|row| row.message))
    .try_collect()
    .await?;
```

You can also build prepared statements via entities for common patterns:
```rust
let mut query = RadioLog::prepare_find(
    executor,
    expr!(RadioLog::signal_strength > ?),
    Some(50),
).await?;
query.bind(40)?;
let messages: Vec<_> = executor
    .fetch(query)
    .map(|row| row.and_then(RadioLog::from_row).map(|log| log.message))
    .try_collect()
    .await?;
```

Prepared statements reuse backend preparation state when available. Parameter conversion failures may surface during binding or execution, depending on the driver.

### Notes & Driver Support
- `SqlWriter::write_create_table(&mut sql, T::table(), if_not_exists)` emits only table DDL. Use `write_create_schema` separately, or `Entity::create_table` to coordinate schema and table creation.
- Streams returned by `executor.run(...)` are ordered by statement execution; interleave `Affected` and `Row` accordingly.
- `executor.fetch(...)` is convenient for row-only workloads, but it discards `Affected` results from mixed batches. Use `run(...)` when you need both result types.
- When selecting a subset of columns, ensure the labels match the entity fields present. Missing labels require `Default` on the entity, not just on the omitted field types.
- Prepared queries use positional parameters across drivers.
- Reuse prepared queries across multiple executions: call `clear_bindings()` to reset parameters and `bind(...)` again before the next shot.
- Prepared queries are backend-owned state. Treat their string formatting as diagnostic output, not as executable SQL.

*Raw fire authorized. Execute with precision. Tank out.*
