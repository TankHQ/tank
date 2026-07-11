# Cheat Sheet

## Connect

### Connection pool
```rust
use tank::PoolConfig;
use tank_postgres::PostgresDriver;

let mut config = PoolConfig::new();
config.max_size = 4;
let pool = PostgresDriver::new()
    .connect_pool("postgres://user:pass@host:5432/db".into(), config)
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
use tank::{ConnectionPool, PoolConfig};
use tank_mysql::MySQLDriver;

let pool: Box<dyn ConnectionPool<MySQLDriver>> = MySQLDriver::mysql()
    .connect_pool("mysql://user:pass@host:3306/db".into(), PoolConfig::new())
    .await?
    .into_box();
```

## Transaction

```rust
use tank::{Connection, expr, Transaction};

let mut tx = connection.begin().await?;
entity.save(&mut tx).await?;
EntityExample::delete_many(&mut tx, expr!(...)).await?;
tx.commit().await?;
```

## Entity Definition

```rust
use std::collections::HashMap;
use tank::Entity;
use uuid::Uuid;

#[derive(Entity, Debug, PartialEq)]
#[tank(
    schema = "army",
    name = "deployments",
    primary_key = (Self::unit_id, Self::region),
)]
struct EntityExample {
    unit_id: Uuid,
    #[tank(clustering_key)]
    region: String,
    #[tank(name = "callsign")]
    callsign: String,
    casualties: i32,
    #[tank(conversion_type = NotesWrap)]
    metadata: Notes,
    #[tank(ignore)]
    transient_cache: HashMap<String, String>,
}
```

`primary_key` is composite: `unit_id` is the partition key and `region` is the clustering key (relevant for Scylla/Cassandra). `clustering_key` is ignored by SQL drivers.
The field transient_cache is ignored by the database (not stored in the table)

### Conversion Types

`conversion_type` lets you use any type as an entity field by routing reads and writes through a local wrapper that implements [`AsValue`](./05-types.md):

```rust
use tank::{AsValue, Error, Result, Value};

#[derive(Debug, PartialEq)]
pub struct Notes(pub String); // Custom third party type
pub struct NotesWrap(pub Notes); // Local wrapper

impl AsValue for NotesWrap {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        Value::Varchar(Some(self.0.0.into()))
    }
    fn try_from_value(value: Value) -> Result<Self> {
        match value.try_as(&Value::Varchar(None)) {
            Ok(Value::Varchar(Some(s))) => {
                Ok(NotesWrap(Notes(s.to_string())))
            }
            _ => Err(anyhow!("Expected Varchar for Notes")),
        }
    }
}
impl From<Notes> for NotesWrap {
    fn from(v: Notes) -> Self {
        NotesWrap(v)
    }
}
impl From<NotesWrap> for Notes {
    fn from(v: NotesWrap) -> Self {
        v.0
    }
}
```

Tank calls `NotesWrap::from(field_value)` when writing and reconstructs the field via `Notes::from(NotesWrap::try_from_value(db_value)?)` when reading. See [Types](./05-types.md) for full `AsValue` documentation and the built-in type table.

## Table setup

```rust
EntityExample::create_table(&mut connection, true, true).await?;
EntityExample::drop_table(&mut connection, true, false).await?;
```

## Insert

```rust
EntityExample::insert_one(&mut connection, &entity).await?;
EntityExample::insert_many(&mut connection, &[entity1, entity2, ...]).await?;
connection.append(&[entity1, entity2, entity3]).await?;
```

## Save and Delete

```rust
entity.save(&mut connection).await?;
entity.delete(&mut connection).await?;
```

> [!NOTE]
> The Entity must have a primary key for this to work.

## Find

```rust
use std::pin::pin;
use tank::{expr, stream::TryStreamExt};

let entity = EntityExample::find_one(
    &mut connection,
    entity.primary_key_expr()
).await?;
let mut stream = pin!(EntityExample::find_many(
    &mut connection,
    expr!(EntityExample::casualties > 0),
    Some(100),
));
while let Some(entity) = stream.try_next().await? {
    println!("{}", entity.callsign);
}
let entities = EntityExample::find_many(
    &mut connection,
    expr!(EntityExample::unit_id == uid),
    None
)
    .try_collect::<Vec<EntityExample>>()
    .await?;
```

## Delete Many

```rust
use tank::expr;
use uuid::Uuid;

EntityExample::delete_many(
    &mut connection,
    expr!(EntityExample::casualties == 0)
).await?;

let uid = Uuid::new_v4();
EntityExample::delete_many(
    &mut connection,
    expr!(EntityExample::unit_id == #uid)
).await?;
```

## Expressions

```rust
use tank::expr;
use uuid::Uuid;

expr!(EntityExample::casualties == 0)
expr!(EntityExample::casualties >= 10)
expr!(EntityExample::region == "North" || EntityExample::region == "South")
expr!(EntityExample::callsign == "Alpha%" as LIKE)
expr!(EntityExample::callsign != "Alpha%" as LIKE)
expr!(EntityExample::casualties > ?)
let uid = Uuid::new_v4();
expr!(EntityExample::unit_id == #uid)
```

## Prepared statement

```rust
use tank::{expr, stream::TryStreamExt};

let mut query = EntityExample::prepare_find(
    &mut connection,
    expr!(EntityExample::unit_id == ?),
    Some(50),
).await?;
query.bind(some_unit_id)?;
let entities = connection.fetch(&mut query)
    .map_ok(|row| EntityExample::from_row(row).unwrap())
    .try_collect::<Vec<EntityExample>>()
    .await?;
query.clear_bindings()?;
query.bind(uid2)?;
```

## Query Builder

```rust
use tank::{cols, expr, stream::TryStreamExt, QueryBuilder};

let results = connection.fetch(
    QueryBuilder::new()
        .select(cols!(EntityExample::callsign, EntityExample::casualties))
        .from(EntityExample::table())
        .where_expr(expr!(EntityExample::casualties > 0))
        .order_by(cols!(EntityExample::casualties DESC))
        .limit(Some(50))
        .build(&connection.driver()),
)
.map_ok(|row| EntityExample::from_row(row).unwrap())
.try_collect::<Vec<_>>()
.await?;
```

## Joins

The `join!` macro builds the `FROM` clause for `QueryBuilder`. Define a result struct that matches the selected columns, then pass the join tree to `.from()`.

Supported keywords: `JOIN`, `INNER JOIN`, `LEFT JOIN`, `LEFT OUTER JOIN`, `RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`.

```rust
use tank::{cols, expr, join, Entity, stream::TryStreamExt, QueryBuilder};

#[derive(Entity, Debug)]
struct BookWithAuthor {
    title: String,
    author: String,
}

let rows: Vec<BookWithAuthor> = connection.fetch(
    QueryBuilder::new()
        .select(cols!(Book::title, Author::name as author))
        .from(join!(Book JOIN Author ON Book::author_id == Author::id))
        .where_expr(expr!(Book::year > 2000))
        .order_by(cols!(Book::title ASC))
        .build(&connection.driver()),
)
.map_ok(BookWithAuthor::from_row)
.map(Result::flatten)
.try_collect()
.await?;

let rows: Vec<BookWithAuthor> = connection.fetch(
    QueryBuilder::new()
        .select(cols!(B.title, A.name as author))
        .from(join!(Book B LEFT JOIN Author A ON B.author_id == A.id))
        .where_expr(true)
        .build(&connection.driver()),
)
.map_ok(BookWithAuthor::from_row)
.map(Result::flatten)
.try_collect()
.await?;

let dataset = join!(
    Book B
        LEFT JOIN Author A1 ON B.author_id == A1.id
        LEFT JOIN Author A2 ON B.co_author_id == A2.id
);
let rows = connection.fetch(
    QueryBuilder::new()
        .select(cols!(B.title, A1.name as author, A2.name as co_author))
        .from(dataset)
        .where_expr(true)
        .build(&connection.driver()),
)
.try_collect::<Vec<_>>()
.await?;
```

## Raw SQL

### Simple query
```rust
use indoc::indoc;
use std::pin::pin;
use tank::{stream::TryStreamExt, QueryResult};

let mut stream = pin!(connection.run(indoc! {"
    SELECT unit_id, callsign
    FROM army.deployments
    WHERE casualties > 0
"}));
while let Some(result) = stream.try_next().await? {
    match result {
        QueryResult::Row(row) => println!("{:?}", row.values),
        QueryResult::Affected(v) => println!("affected: {}", v.rows_affected),
    }
}
let rows: Vec<_> = connection
    .fetch("SELECT * FROM army.deployments")
    .try_collect()
    .await?;
let affected = connection.execute(indoc! {"
    UPDATE army.deployments SET casualties = 0
    WHERE region = 'North'"
}).await?;
```

### Prepared
```rust
use indoc::indoc;
use tank::{Entity, stream::TryStreamExt};

let mut query = connection.prepare(indoc! {"
    SELECT unit_id, callsign
    FROM army.deployments
    WHERE unit_id = ?
    LIMIT ?
"}.into()
).await?;
query.bind(uid)?;
query.bind(25)?;

let rows = connection.fetch(&mut query).try_collect::<Vec<_>>().await?;
let entity = EntityExample::from_row(row)?;

#[derive(Entity)]
struct Slim { callsign: String, casualties: i32 }
let slim = Slim::from_row(row)?;
query.clear_bindings()?;
query.bind(other_uid)?;
query.bind(10)?;
```

### SqlWriter

```rust
use tank::{DynQuery, QueryBuilder, QueryResult, SqlWriter, stream::TryStreamExt};

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

let results: Vec<QueryResult> = connection.run(query).try_collect().await?;
```
