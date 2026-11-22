# Cheat Sheet

## Connect

### Connection pool
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_postgres::PostgresDriver;

let mut config = PoolConfig::new();
config.max_size = 4;
let pool = PostgresDriver::new()
    .connect_pool("postgres://user:pass@host:5432/db", config)
    .await?;
let mut connection = pool.get().await?;
```

### Single connection
```rust
use tank::Connection;
use tank_sqlite::{SQLiteConnection, SQLiteDriver};

let driver = SQLiteDriver::new();
let mut connection = SQLiteConnection::connect(
    &driver,
    "sqlite:///path/to/db.sqlite?mode=rwc".into(),
).await?;
connection.disconnect().await?;
```

### Type-erased pool
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_mysql::MySQLDriver;

let pool = MySQLDriver::mysql()
    .connect_pool("mysql://user:pass@host:3306/db", PoolConfig::new())
    .await?
    .into_arc();
```

## Entity Definition

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-entity [Rust]

`primary_key` is composite: `unit_id` is the partition key and `region` is the clustering key (relevant for Scylla/Cassandra). `clustering_key` is ignored by SQL drivers.
The `transient_cache` field is not stored in the table.

### Conversion Types

`conversion_type` routes field reads and writes through a local wrapper implementing [`AsValue`](05-types.md) and `From` conversions to and from the cloneable field type:

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-conversion [Rust]

Tank calls `NotesWrap::from(field_value)` when writing and reconstructs the field via `Notes::from(NotesWrap::decode_value(db_value)?)` when reading. See [Types](05-types.md) for full `AsValue` documentation and the built-in type table.

## Table setup

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-table-setup [Rust]

## Insert

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-insert [Rust]

`insert_many` and `append` accept any iterable yielding entity values or references.

## Save and Delete

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-save-delete [Rust]

> [!NOTE]
> The Entity must have a primary key for this to work. It adds a `ON CONFLICT` clause that turns the insertion into a update statement.

## Find

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-find [Rust]

## Delete Many

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-delete-many [Rust]

## Transaction

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-transaction [Rust]

Both transactions and connections can serve as executors in methods such as `EntityExample::create_table(&mut tx, ...)`.

## Expressions

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-expressions [Rust]

## Prepared statement

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-prepared [Rust]

## Query Builder

Call `tank::Sql()` to start a typed builder for the operations that go beyond the entity helpers.

Finish a builder with `.build(&driver)`, which returns a query for that driver ready for `run`, `fetch` or `execute` on the executor.

Call `.boxed()` instead to erase the driver and get a [`BoxQueryBuilder`](https://docs.rs/tank/latest/tank/struct.BoxQueryBuilder.html): store it then run it later with any executor (of different drivers).

To append a statement to an existing [`AnyQuery`](https://docs.rs/tank/latest/tank/enum.AnyQuery.html) buffer, call `.write_into(&driver.sql_writer(), &mut out)`.

### Select

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-query-builder [Rust]

Also available: `.distinct()`, `.group_by(...)`, `.having(...)` and `.offset(...)`. See [Advanced operations](08-advanced-operations.md#selecting-ordering).

### Insert

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-builder-insert [Rust]

Use `.columns([...]).values([cols!(?, ?)])` for explicit rows and `.select(...)` to insert the rows of a query.

### Update

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-builder-update [Rust]

Each `cols!` item written with `==` becomes a `SET` assignment. `entity.primary_key_expr()` targets the row. Without `where_expr`, every row is updated. MongoDB and Valkey do not support plain `UPDATE`. Use `save()` there.

### Delete

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-builder-delete [Rust]

### Create table

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-builder-create-table [Rust]

### Drop table

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-builder-drop-table [Rust]

## Joins

The `join!` macro builds the `FROM` clause of a `tank::Sql` query. Define a result struct that matches the selected columns, then pass the join tree to `.from()`.

Supported keywords: `JOIN`, `INNER JOIN`, `LEFT JOIN`, `LEFT OUTER JOIN`, `RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`.

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-joins [Rust]

## Raw SQL

### Simple query
```rust
use indoc::indoc;
use std::pin::pin;
use tank::{Row, stream::TryStreamExt};

{
    let mut stream = pin!(connection.run(indoc! {r#"
        SELECT unit_id, callsign
        FROM army.deployments
        WHERE casualties > 0
    "#}));
    while let Some(result) = stream.try_next().await? {
        match result {
            tank::QueryResult::Row(row) => {
                println!("{:?}", row.value(0));
            }
            tank::QueryResult::Affected(v) => {
                println!("affected: {:?}", v.rows_affected);
            }
        }
    }
}
let rows: Vec<_> = connection
    .fetch("SELECT * FROM army.deployments")
    .map_ok(Row::into_owned)
    .try_collect()
    .await?;
let affected = connection
    .execute(indoc! {r#"
        UPDATE army.deployments SET casualties = 0
        WHERE region = 'North'
    "#})
    .await?;
```

### Prepared

This raw SQL uses PostgreSQL's `$1`, `$2` placeholders. Other drivers, such as DuckDB and MySQL will use `?`.

```rust
use indoc::indoc;
use tank::stream::TryStreamExt;

let mut query = connection.prepare(indoc! {r#"
    SELECT unit_id, region, callsign, casualties, metadata
    FROM army.deployments
    WHERE unit_id = $1
    LIMIT $2
"#}).await?;
query.bind(uid)?;
query.bind(25)?;
let rows = connection.fetch(&mut query).try_collect::<Vec<_>>().await?;
for row in rows {
    let entity = EntityExample::from_row(&row)?;
    let slim = Slim::from_row(&row)?;
}

// Clear, rebing and repeat
query.clear_bindings()?;
query.bind(other_uid)?;
query.bind(10)?;
connection.fetch(&mut query)...
```

### SqlWriter

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-sql-writer [Rust]

## Common workflows

### Streams

`fetch` and `find_many` return a [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) of `Result`s, so the `TryStreamExt` combinators apply: `and_then` / `map_ok` to decode rows, `try_filter` to drop items, `try_fold` to aggregate, `try_collect` to gather a `Vec`, `run` also yields `QueryResult::Affected` items for statements that modify rows.

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-streams [Rust]

> [!TIP]
> Filtering on the Rust side transfers and decodes every row: express the condition in `expr!` whenever the database can evaluate it.
