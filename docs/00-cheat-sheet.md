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
use std::sync::Arc;
use tank::{Driver, ConnectionPool, PoolConfig};
use tank_mysql::MySQLDriver;

let pool: Arc<dyn ConnectionPool<MySQLDriver>> = MySQLDriver::mysql()
    .connect_pool("mysql://user:pass@host:3306/db", PoolConfig::new())
    .await?
    .into_arc();
```

## Entity Definition

```rust
use std::collections::HashMap;
use tank::Entity;
use uuid::Uuid;

<!--@include: @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-entity-->
```

`primary_key` is composite: `unit_id` is the partition key and `region` is the clustering key (relevant for Scylla/Cassandra). `clustering_key` is ignored by SQL drivers.
The `transient_cache` field is not stored in the table.

### Conversion Types

`conversion_type` routes field reads and writes through a local wrapper implementing [`AsValue`](05-types.md) and `From` conversions to and from the cloneable field type:

```rust
use tank::{AsValue, Entity, Result, Value, ValueOwned, anyhow};

<!--@include: @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-conversion-->
```

Tank calls `NotesWrap::from(field_value)` when writing and reconstructs the field via `Notes::from(NotesWrap::try_from_value(db_value)?)` when reading. See [Types](05-types.md) for full `AsValue` documentation and the built-in type table.

## Table setup

```rust
EntityExample::create_table(&mut connection, true, true).await?;
EntityExample::drop_table(&mut connection, true, false).await?;
```

## Insert

```rust
EntityExample::insert_one(&mut connection, &entity).await?;
EntityExample::insert_many(&mut connection, [&entity2, ...]).await?;
connection.append([&entity3, ...]).await?;
```

`insert_many` and `append` accept any iterable yielding entity values or references.

## Save and Delete

```rust
entity.save(&mut connection).await?;
entity.delete(&mut connection).await?;
```

> [!NOTE]
> The Entity must have a primary key for this to work.

## Find

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-find [Rust]

## Delete Many

```rust
use tank::{Entity, expr};

let uid = entity2.unit_id;
EntityExample::delete_many(
    &mut connection,
    expr!(EntityExample::unit_id == #uid)
).await?;

let uid = entity3.unit_id;
EntityExample::delete_many(
    &mut connection,
    expr!(EntityExample::unit_id == #uid)
).await?;
```

## Transaction

```rust
use tank::{Entity, Transaction};

let mut tx = connection.begin().await?;
EntityExample::insert_one(&mut tx, &entity).await?;
entity.delete(&mut tx).await?;
tx.commit().await?; // Or tx.rollback().await?
```

Both transactions and connections can serve as executors in methods such as `EntityExample::create_table(&mut tx, ...)`.

## Expressions

```rust
use tank::expr;
use uuid::Uuid;

expr!(EntityExample::casualties == 0);
expr!(EntityExample::casualties >= 10);
expr!(EntityExample::region == "North" || EntityExample::region == "South");
expr!(EntityExample::callsign == "Alpha%" as LIKE);
expr!(EntityExample::callsign != "Alpha%" as LIKE);
expr!(EntityExample::casualties > ?);
let uid = Uuid::new_v4();
expr!(EntityExample::unit_id == #uid);
let regions = ["North", "South"];
expr!(EntityExample::region == #regions as IN);
```

## Prepared statement

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-prepared [Rust]

## Query Builder

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-query-builder [Rust]

## Joins

The `join!` macro builds the `FROM` clause for `QueryBuilder`. Define a result struct that matches the selected columns, then pass the join tree to `.from()`.

Supported keywords: `JOIN`, `INNER JOIN`, `LEFT JOIN`, `LEFT OUTER JOIN`, `RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`.

<<< @/../tank-tests/src/cheat_sheet.rs#cheat-sheet-joins [Rust]

## Raw SQL

### Simple query
```rust
use indoc::indoc;
use std::pin::pin;
use tank::{QueryResult, Row, stream::TryStreamExt};

{
    let mut stream = pin!(connection.run(indoc! (r#"
        SELECT unit_id, callsign
        FROM army.deployments
        WHERE casualties > 0
    "#)));
    while let Some(result) = stream.try_next().await? {
        match result {
            QueryResult::Row(row) => {
                println!("{:?}", row.value(0));
            }
            QueryResult::Affected(v) => {
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
use tank::{Entity, stream::TryStreamExt};

let mut query = connection.prepare(indoc! (r#"
    SELECT unit_id, region, callsign, casualties, metadata
    FROM army.deployments
    WHERE unit_id = $1
    LIMIT $2
"#)).await?;
query.bind(uid)?;
query.bind(25)?;

let rows = connection.fetch(&mut query).try_collect::<Vec<_>>().await?;

for row in rows {
    let entity = EntityExample::from_row(&row)?;
    let slim = Slim::from_row(&row)?;
}
query.clear_bindings()?;
query.bind(other_uid)?;
query.bind(10)?;
```

### SqlWriter

```rust
use tank::{DynQuery, QueryBuilder, SqlWriter, stream::TryStreamExt};

let writer = connection.driver().sql_writer();
let mut query = DynQuery::default();

writer.write_create_table::<EntityExample>(&mut query, true);
writer.write_insert(&mut query, &[entity1, entity2], false);
writer.write_select(
    &mut query,
    &QueryBuilder::new()
        .select(EntityExample::columns())
        .from(EntityExample::table())
        .where_expr(true)
        .limit(Some(100)),
);

let results: Vec<_> = connection.run(query).try_collect().await?;
```

## Common workflows
