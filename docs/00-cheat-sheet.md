# Cheat Sheet

## Connect

### Connection pool
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
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
use std::sync::Arc;
use tank::{Driver, ConnectionPool, PoolConfig};
use tank_mysql::MySQLDriver;

let pool: Arc<dyn ConnectionPool<MySQLDriver>> = MySQLDriver::mysql()
    .connect_pool("mysql://user:pass@host:3306/db".into(), PoolConfig::new())
    .await?
    .into_arc();
```

## Entity Definition

```rust
use std::collections::HashMap;
use tank::Entity;
use uuid::Uuid;

#[derive(Entity)]
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
The `transient_cache` field is not stored in the table.

### Conversion Types

`conversion_type` routes field reads and writes through a local wrapper implementing [`AsValue`](05-types.md) and `From` conversions to and from the cloneable field type:

```rust
use tank::{AsValue, Entity, Result, Value, ValueOwned, anyhow};

#[derive(Clone)]
pub struct Notes(pub String); // Third party type
pub struct NotesWrap(pub Notes); // Local wrapper

impl AsValue for NotesWrap {
    fn as_empty_value() -> ValueOwned {
        Value::Varchar(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(self.0.0.into()))
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        match value.as_value().try_as(&Value::Varchar(None)) {
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

Tank calls `NotesWrap::from(field_value)` when writing and reconstructs the field via `Notes::from(NotesWrap::try_from_value(db_value)?)` when reading. See [Types](05-types.md) for full `AsValue` documentation and the built-in type table.

## Table setup

```rust
EntityExample::create_table(&mut connection, true, true).await?;
EntityExample::drop_table(&mut connection, true, false).await?;
```

## Transaction

```rust
use tank::{Entity, Transaction};

let mut tx = connection.begin().await?;
EntityExample::insert_one(&mut tx, &entity).await?;
entity.delete(&mut tx).await?;
tx.commit().await?;
```

Both transactions and connections can serve as executors in methods such as `EntityExample::create_table(&mut tx, ...)`.

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

```rust
use std::pin::pin;
use tank::{Entity, expr, stream::TryStreamExt};

let entity = EntityExample::find_one(
    &mut connection,
    entity.primary_key_expr()
).await?;
{
    let uid = entity2.unit_id;
    let mut stream = pin!(EntityExample::find_many(
        &mut connection,
        expr!(EntityExample::unit_id == #uid),
        Some(100),
    ));
    while let Some(entity) = stream.try_next().await? {
        println!("{}", entity.callsign);
    }
}
let uid = Uuid::parse_str("94f0cbcc-1fce-454e-a6e4-4e3587741808")?;
let entities: Vec<EntityExample> =
    EntityExample::find_many(
        &mut connection,
        expr!(EntityExample::unit_id == #uid),
        None
    )
    .try_collect()
    .await?;
```

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

```rust
use tank::{Entity, expr, stream::{StreamExt, TryStreamExt}};

EntityExample::insert_one(&mut connection, &entity).await?;
let mut query = EntityExample::prepare_find(
    &mut connection,
    expr!(EntityExample::unit_id == ?),
    Some(50),
)
.await?;
query.bind(entity.unit_id)?;
let entities = connection
    .fetch(&mut query)
    .map(|row| row.and_then(EntityExample::from_row))
    .try_collect::<Vec<EntityExample>>()
    .await?;
query.clear_bindings()?;
query.bind(entity2.unit_id)?;
```

## Query Builder

```rust
use tank::{cols, expr, stream::{StreamExt, TryStreamExt}, QueryBuilder};

#[derive(Entity)]
struct Slim { callsign: String, casualties: i32 }

let uid = entity.unit_id;
let results = connection.fetch(
    QueryBuilder::new()
        .select(cols!(EntityExample::callsign, EntityExample::casualties))
        .from(EntityExample::table())
        .where_expr(expr!(EntityExample::unit_id == #uid))
        .order_by(cols!(EntityExample::region ASC))
        .limit(Some(50))
        .build(&connection.driver()),
)
.map(|row| row.and_then(Slim::from_row))
.try_collect::<Vec<_>>()
.await?;
```

## Joins

The `join!` macro builds the `FROM` clause for `QueryBuilder`. Define a result struct that matches the selected columns, then pass the join tree to `.from()`.

Supported keywords: `JOIN`, `INNER JOIN`, `LEFT JOIN`, `LEFT OUTER JOIN`, `RIGHT JOIN`, `RIGHT OUTER JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`.

```rust
use tank::{
    Entity, QueryBuilder, cols, expr, join, stream::StreamExt, stream::TryStreamExt,
};

#[derive(Entity)]
struct BookWithAuthor {
    title: String,
    author: String,
}

let rows: Vec<BookWithAuthor> = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(Book::title, Author::name as author))
            .from(join!(Book JOIN Author ON Book::author == Author::id))
            .where_expr(expr!(Book::year > 2000))
            .order_by(cols!(Book::title ASC))
            .build(&connection.driver()),
    )
    .map_ok(BookWithAuthor::from_row)
    .map(Result::flatten)
    .try_collect()
    .await?;

let rows: Vec<BookWithAuthor> = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(B.title, A.name as author))
            .from(join!(Book B LEFT JOIN Author A ON B.author == A.author_id))
            .where_expr(true)
            .build(&connection.driver()),
    )
    .map_ok(BookWithAuthor::from_row)
    .map(Result::flatten)
    .try_collect()
    .await?;

let dataset = join!(
    Book B
        LEFT JOIN Author A1 ON B.author == A1.author_id
        LEFT JOIN Author A2 ON B.co_author == A2.author_id
);
let rows = connection
    .fetch(
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
use tank::{QueryResult, Row, stream::TryStreamExt};

{
    let mut stream = pin!(connection.run(indoc! {r#"
    SELECT unit_id, callsign
    FROM army.deployments
    WHERE casualties > 0
"#}));
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

let mut query = connection.prepare(indoc! {"
    SELECT unit_id, region, callsign, casualties, metadata
    FROM army.deployments
    WHERE unit_id = $1
    LIMIT $2
"}
).await?;
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
