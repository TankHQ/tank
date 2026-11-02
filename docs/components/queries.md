<CodeWindow title="create_table.rs">

```rust
use tank::Entity;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "deployments")]
struct Deployment {
    #[tank(primary_key)]
    unit_id: Uuid,
    callsign: String,
    crew: u8,
    casualties: i32,
    fuel_ratio: f32,
    #[tank(name = "operational")]
    is_operational: bool,
    supply_ratio: Option<Decimal>,
}

Deployment::create_table(&mut connection, true, true).await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
CREATE SCHEMA IF NOT EXISTS "army";
CREATE TABLE IF NOT EXISTS "army"."deployments" (
    "unit_id" UUID PRIMARY KEY,
    "callsign" TEXT NOT NULL,
    "crew" SMALLINT NOT NULL,
    "casualties" INTEGER NOT NULL,
    "fuel_ratio" FLOAT4 NOT NULL,
    "operational" BOOLEAN NOT NULL,
    "supply_ratio" NUMERIC
);
```
```sql [MySQL]
CREATE SCHEMA IF NOT EXISTS `army`;
CREATE TABLE IF NOT EXISTS `army`.`deployments` (
    `unit_id` CHAR(36) PRIMARY KEY,
    `callsign` TEXT NOT NULL,
    `crew` TINYINT UNSIGNED NOT NULL,
    `casualties` INTEGER NOT NULL,
    `fuel_ratio` FLOAT NOT NULL,
    `operational` BOOLEAN NOT NULL,
    `supply_ratio` DECIMAL
);
```
```sql [SQLite]
CREATE TABLE IF NOT EXISTS "deployments" (
    "unit_id" TEXT PRIMARY KEY,
    "callsign" TEXT NOT NULL,
    "crew" INTEGER NOT NULL,
    "casualties" INTEGER NOT NULL,
    "fuel_ratio" REAL NOT NULL,
    "operational" INTEGER NOT NULL,
    "supply_ratio" REAL
);
```
```sql [DuckDB]
CREATE SCHEMA IF NOT EXISTS "army";
CREATE TABLE IF NOT EXISTS "army"."deployments" (
    "unit_id" UUID PRIMARY KEY,
    "callsign" VARCHAR NOT NULL,
    "crew" UTINYINT NOT NULL,
    "casualties" INTEGER NOT NULL,
    "fuel_ratio" FLOAT NOT NULL,
    "operational" BOOLEAN NOT NULL,
    "supply_ratio" DECIMAL
);
```
```sql [ClickHouse]
CREATE DATABASE IF NOT EXISTS `army`;
CREATE TABLE IF NOT EXISTS `army`.`deployments` (
    `unit_id` UUID,
    `callsign` String,
    `crew` UInt8,
    `casualties` Int32,
    `fuel_ratio` Float32,
    `operational` Bool,
    `supply_ratio` Nullable(Decimal(38,9))
)
ENGINE = ReplacingMergeTree()
ORDER BY (`unit_id`);
```
```sql [ScyllaDB]
CREATE KEYSPACE IF NOT EXISTS "army"
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
CREATE TABLE IF NOT EXISTS "army"."deployments" (
    "unit_id" UUID PRIMARY KEY,
    "callsign" TEXT NOT NULL,
    "crew" SMALLINT NOT NULL,
    "casualties" INT NOT NULL,
    "fuel_ratio" FLOAT NOT NULL,
    "operational" BOOLEAN NOT NULL,
    "supply_ratio" DECIMAL
);
```
:::

<CodeWindow title="drop_table.rs">

```rust
use tank::Entity;

#[derive(Entity)]
#[tank(schema = "army", name = "deployments")]
struct Deployment {}

Deployment::drop_table(&mut connection, true, false).await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
DROP TABLE IF EXISTS "army"."deployments";
```
```sql [MySQL]
DROP TABLE IF EXISTS `army`.`deployments`;
```
```sql [SQLite]
DROP TABLE IF EXISTS "deployments";
```
:::

<CodeWindow title="insert_one.rs">

```rust
use tank::Entity;

#[derive(Entity, Default)]
#[tank(schema = "army", name = "vehicles")]
struct Vehicle {
    #[tank(primary_key)]
    id: i64,
    model: String,
    crew: u8,
    weight_kg: u32,
    in_service: bool,
}

let vehicle = Vehicle { id: 1, model: "Tiger I".into(), ..Default::default() };

Vehicle::insert_one(&mut connection, &vehicle).await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
INSERT INTO "army"."vehicles" ("id", "model", "crew", "weight_kg", "in_service") VALUES
(1, 'Tiger I', 5, 57000, true);
```
```sql [MySQL]
INSERT INTO `army`.`vehicles` (`id`, `model`, `crew`, `weight_kg`, `in_service`) VALUES
(1, 'Tiger I', 5, 57000, true);
```
```sql [SQLite]
INSERT INTO "vehicles" ("id", "model", "crew", "weight_kg", "in_service") VALUES
(1, 'Tiger I', 5, 57000, true);
```
:::

<CodeWindow title="insert_many.rs">

```rust
use tank::Entity;
use uuid::Uuid;

#[derive(Entity, Default)]
#[tank(schema = "army", name = "soldiers")]
struct Soldier {
    #[tank(primary_key)]
    id: Uuid,
    name: String,
    rank: String,
    age: u8,
}

let rook = Soldier { id: Uuid::new_v4(), name: "Rook".into(), ..Default::default() };
let viper = Soldier { id: Uuid::new_v4(), name: "Viper".into(), ..Default::default() };

Soldier::insert_many(&mut connection, &[rook, viper]).await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
INSERT INTO "army"."soldiers" ("id", "name", "rank", "age") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'SteelHammer', 'Major', 41),
('2f4f97da-0278-4c99-bc22-2b3986aeee85', 'Viper', 'Sgt', 33);
```
```sql [MySQL]
INSERT INTO `army`.`soldiers` (`id`, `name`, `rank`, `age`) VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'SteelHammer', 'Major', 41),
('2f4f97da-0278-4c99-bc22-2b3986aeee85', 'Viper', 'Sgt', 33);
```
```sql [SQLite]
INSERT INTO "soldiers" ("id", "name", "rank", "age") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'SteelHammer', 'Major', 41),
('2f4f97da-0278-4c99-bc22-2b3986aeee85', 'Viper', 'Sgt', 33);
```
:::

<CodeWindow title="save.rs">

```rust
use tank::Entity;
use uuid::Uuid;

#[derive(Entity, Default)]
#[tank(schema = "army", name = "missions")]
struct Mission {
    #[tank(primary_key)]
    id: Uuid,
    code_name: String,
    progress: f32,
    complete: bool,
}

let mut mission = Mission { id: Uuid::new_v4(), code_name: "Red Dawn".into(), ..Default::default() };

// Insert, or update when the primary key already exists.
mission.progress = 0.4;
mission.save(&mut connection).await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
INSERT INTO "army"."missions" ("id", "code_name", "progress", "complete") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Red Dawn', 0.4, true)
ON CONFLICT ("id") DO UPDATE SET
"code_name" = EXCLUDED."code_name",
"progress" = EXCLUDED."progress",
"complete" = EXCLUDED."complete";
```
```sql [MySQL]
INSERT INTO `army`.`missions` (`id`, `code_name`, `progress`, `complete`) VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Red Dawn', 0.4, true)
ON DUPLICATE KEY UPDATE
`code_name` = VALUES(`code_name`),
`progress` = VALUES(`progress`),
`complete` = VALUES(`complete`);
```
```sql [SQLite]
INSERT INTO "missions" ("id", "code_name", "progress", "complete") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Red Dawn', 0.4, true)
ON CONFLICT ("id") DO UPDATE SET
"code_name" = EXCLUDED."code_name",
"progress" = EXCLUDED."progress",
"complete" = EXCLUDED."complete";
```
:::

<CodeWindow title="delete_many.rs">

```rust
use tank::{Entity, expr};
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "signals")]
struct Signal {
    #[tank(primary_key)]
    id: Uuid,
    freq_mhz: f64,
    strength: i8,
    encrypted: bool,
}

Signal::delete_many(
    &mut connection,
    expr!(Signal::encrypted == false),
)
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
DELETE FROM "army"."signals"
WHERE "encrypted" = false;
```
```sql [MySQL]
DELETE FROM `army`.`signals`
WHERE `encrypted` = false;
```
```sql [SQLite]
DELETE FROM "signals"
WHERE "encrypted" = false;
```
:::

<CodeWindow title="find_one.rs">

```rust
use tank::{Entity, expr};
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "operators")]
struct Operator {
    #[tank(primary_key)]
    id: Uuid,
    callsign: String,
    rank: String,
}

let uid = Uuid::new_v4();
let unit = Operator::find_one(
    &mut connection,
    expr!(Operator::id == #uid),
)
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "id", "callsign", "rank"
FROM "army"."operators"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808'
LIMIT 1;
```
```sql [MySQL]
SELECT `id`, `callsign`, `rank`
FROM `army`.`operators`
WHERE `id` = '94f0cbcc-1fce-454e-a6e4-4e3587741808'
LIMIT 1;
```
```sql [SQLite]
SELECT "id", "callsign", "rank"
FROM "operators"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808'
LIMIT 1;
```
:::

<CodeWindow title="find_many.rs">

```rust
use tank::{Entity, expr, stream::TryStreamExt};
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "drones")]
struct Drone {
    #[tank(primary_key)]
    id: Uuid,
    model: String,
    battery_pct: u8,
    active: bool,
}

let flying: Vec<Drone> = Drone::find_many(
    &mut connection,
    expr!(Drone::active == true
        && Drone::battery_pct > 20),
    Some(50),
)
.try_collect()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "id", "model", "battery_pct", "active"
FROM "army"."drones"
WHERE "active" = true AND "battery_pct" > 20
LIMIT 50;
```
```sql [MySQL]
SELECT `id`, `model`, `battery_pct`, `active`
FROM `army`.`drones`
WHERE `active` = true AND `battery_pct` > 20
LIMIT 50;
```
```sql [SQLite]
SELECT "id", "model", "battery_pct", "active"
FROM "drones"
WHERE "active" = true AND "battery_pct" > 20
LIMIT 50;
```
:::

<CodeWindow title="prepare.rs">

```rust
use tank::{Entity, expr, stream::TryStreamExt};
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "messages")]
struct Message {
    #[tank(primary_key)]
    id: Uuid,
    sender: Uuid,
    body: String,
    sent_at: DateTime,
}

let mut query = Message::prepare_find(
    &mut connection,
    expr!(Message::sender == ?),
    Some(50),
)
.await?;

query.bind(Uuid::new_v4())?;
let rows = connection.fetch(&mut query).try_collect::<Vec<_>>().await?;
query.clear_bindings()?;
query.bind(Uuid::new_v4())?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "id", "sender", "body", "sent_at"
FROM "army"."messages"
WHERE "sender" = $1
LIMIT 50;
```
```sql [MySQL]
SELECT `id`, `sender`, `body`, `sent_at`
FROM `army`.`messages`
WHERE `sender` = ?
LIMIT 50;
```
```sql [SQLite]
SELECT "id", "sender", "body", "sent_at"
FROM "messages"
WHERE "sender" = ?
LIMIT 50;
```
:::

<CodeWindow title="expr.rs">

```rust
use tank::{Entity, expr, stream::TryStreamExt};

#[derive(Entity)]
#[tank(schema = "army", name = "metrics")]
struct Metric {
    #[tank(primary_key)]
    id: i64,
    name: String,
    country: String,
    value: f64,
}

let countries = ["NL", "UK"];
let rows: Vec<Metric> = Metric::find_many(
    &mut connection,
    expr!(Metric::name == "income%" as LIKE
        && Metric::country == #countries as IN
        && Metric::value > 0.0),
    None,
)
.try_collect()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "id", "name", "country", "value"
FROM "army"."metrics"
WHERE "name" LIKE 'income%' AND "country" IN ('NL', 'UK') AND "value" > 0.0;
```
```sql [MySQL]
SELECT `id`, `name`, `country`, `value`
FROM `army`.`metrics`
WHERE `name` LIKE 'income%' AND `country` IN ('NL', 'UK') AND `value` > 0.0;
```
```sql [SQLite]
SELECT "id", "name", "country", "value"
FROM "metrics"
WHERE "name" LIKE 'income%' AND "country" IN ('NL', 'UK') AND "value" > 0.0;
```
:::

<CodeWindow title="query_builder.rs">

```rust
use tank::{Entity, QueryBuilder, cols, expr};

#[derive(Entity)]
#[tank(schema = "army", name = "readings")]
struct Reading {
    #[tank(primary_key)]
    id: i64,
    sensor: String,
    celsius: f32,
    humidity: f32,
}

let hottest: Vec<Reading> = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(Reading::sensor, Reading::celsius))
            .from(Reading::table())
            .where_expr(expr!(Reading::celsius > 30.0))
            .order_by(cols!(Reading::celsius DESC))
            .limit(Some(10))
            .build(&connection.driver()),
    )
.try_collect()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "sensor", "celsius"
FROM "army"."readings"
WHERE "celsius" > 30.0
ORDER BY "celsius" DESC
LIMIT 10;
```
```sql [MySQL]
SELECT `sensor`, `celsius`
FROM `army`.`readings`
WHERE `celsius` > 30.0
ORDER BY `celsius` DESC
LIMIT 10;
```
```sql [SQLite]
SELECT "sensor", "celsius"
FROM "readings"
WHERE "celsius" > 30.0
ORDER BY "celsius" DESC
LIMIT 10;
```
:::

<CodeWindow title="group_by.rs">

```rust
use tank::{Entity, QueryBuilder, cols};
use rust_decimal::Decimal;

#[derive(Entity)]
#[tank(schema = "trading", name = "trades")]
struct Trade {
    #[tank(primary_key)]
    id: i64,
    symbol: String,
    venue: String,
    price: Decimal,
    quantity: u32,
}

let by_symbol: Vec<Trade> = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(
                Trade::symbol,
                COUNT(*) as fills,
                SUM(Trade::quantity) as volume,
            ))
            .from(Trade::table())
            .group_by([Trade::symbol])
            .order_by(cols!(Trade::symbol ASC))
            .build(&connection.driver()),
    )
.try_collect()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "symbol", COUNT(*) AS fills, SUM("quantity") AS volume
FROM "trading"."trades"
GROUP BY "symbol"
ORDER BY "symbol" ASC;
```
```sql [MySQL]
SELECT `symbol`, COUNT(*) AS fills, SUM(`quantity`) AS volume
FROM `trading`.`trades`
GROUP BY `symbol`
ORDER BY `symbol` ASC;
```
```sql [SQLite]
SELECT "symbol", COUNT(*) AS fills, SUM("quantity") AS volume
FROM "trades"
GROUP BY "symbol"
ORDER BY "symbol" ASC;
```
:::

<CodeWindow title="having.rs">

```rust
use tank::{Entity, QueryBuilder, cols, expr};

#[derive(Entity)]
#[tank(schema = "ops", name = "events")]
struct Event {
    #[tank(primary_key)]
    id: i64,
    source: String,
    level: String,
    count: u32,
}

let noisy: Vec<Event> = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(Event::level, SUM(Event::count) as total))
            .from(Event::table())
            .group_by([Event::level])
            .having(expr!(SUM(Event::count) > 1000))
            .order_by(cols!(Event::level ASC))
            .build(&connection.driver()),
    )
.try_collect()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "level", SUM("count") AS total
FROM "ops"."events"
GROUP BY "level"
HAVING SUM("count") > 1000
ORDER BY "level" ASC;
```
```sql [MySQL]
SELECT `level`, SUM(`count`) AS total
FROM `ops`.`events`
GROUP BY `level`
HAVING SUM(`count`) > 1000
ORDER BY `level` ASC;
```
```sql [SQLite]
SELECT "level", SUM("count") AS total
FROM "events"
GROUP BY "level"
HAVING SUM("count") > 1000
ORDER BY "level" ASC;
```
:::

<CodeWindow title="join.rs">

```rust
use tank::{Entity, QueryBuilder, cols, join};

#[derive(Entity)]
#[tank(schema = "library", name = "books")]
struct Book {
    #[tank(primary_key)]
    id: i64,
    title: String,
    author_id: i64,
}

#[derive(Entity)]
#[tank(schema = "library", name = "authors")]
struct Author {
    #[tank(primary_key)]
    id: i64,
    name: String,
}

let rows = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(Book::title, Author::name))
            .from(join!(
                Book JOIN Author
                    ON Book::author_id == Author::id
            ))
            .build(&connection.driver()),
    )
.try_collect::<Vec<_>>()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "library"."books"."title", "library"."authors"."name"
FROM "library"."books"
JOIN "library"."authors"
    ON "library"."books"."author_id" = "library"."authors"."id";
```
```sql [MySQL]
SELECT `library`.`books`.`title`, `library`.`authors`.`name`
FROM `library`.`books`
JOIN `library`.`authors`
    ON `library`.`books`.`author_id` = `library`.`authors`.`id`;
```
```sql [SQLite]
SELECT "books"."title", "authors"."name"
FROM "books"
JOIN "authors"
    ON "books"."author_id" = "authors"."id";
```
:::

<CodeWindow title="left_join.rs">

```rust
use tank::{Entity, QueryBuilder, cols, expr, join};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "shop", name = "orders")]
struct Order {
    #[tank(primary_key)]
    id: Uuid,
    customer_id: Uuid,
    total: Decimal,
}

#[derive(Entity)]
#[tank(schema = "shop", name = "customers")]
struct Customer {
    #[tank(primary_key)]
    id: Uuid,
    name: String,
}

let rows = connection
    .fetch(
        QueryBuilder::new()
            .select(cols!(O.total, C.name as customer))
            .from(join!(
                Order O LEFT JOIN Customer C
                    ON O::customer_id == C::id
            ))
            .where_expr(expr!(O::total > 100.0))
            .order_by(cols!(O::total DESC))
            .build(&connection.driver()),
    )
.try_collect::<Vec<_>>()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT O."total", C."name" AS customer
FROM "shop"."orders" O
LEFT JOIN "shop"."customers" C
    ON O."customer_id" = C."id"
WHERE O."total" > 100.0
ORDER BY O."total" DESC;
```
```sql [MySQL]
SELECT O.`total`, C.`name` AS customer
FROM `shop`.`orders` O
LEFT JOIN `shop`.`customers` C
    ON O.`customer_id` = C.`id`
WHERE O.`total` > 100.0
ORDER BY O.`total` DESC;
```
```sql [SQLite]
SELECT O."total", C."name" AS customer
FROM "orders" O
LEFT JOIN "customers" C
    ON O."customer_id" = C."id"
WHERE O."total" > 100.0
ORDER BY O."total" DESC;
```
:::

<CodeWindow title="transaction.rs">

```rust
use tank::{Entity, Transaction};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Entity, Default)]
#[tank(schema = "bank", name = "payments")]
struct Payment {
    #[tank(primary_key)]
    id: Uuid,
    account: String,
    amount: Decimal,
    settled: bool,
}

let payment = Payment { id: Uuid::new_v4(), amount: Decimal::new(2599, 2), ..Default::default() };

let mut tx = connection.begin().await?;
Payment::insert_one(&mut tx, &payment).await?;
payment.delete(&mut tx).await?;
tx.commit().await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
BEGIN;
INSERT INTO "bank"."payments" ("id", "account", "amount", "settled") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'NL91ABNA0417164300', 25.99, true);
DELETE FROM "bank"."payments"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808';
COMMIT;
```
```sql [MySQL]
BEGIN;
INSERT INTO `bank`.`payments` (`id`, `account`, `amount`, `settled`) VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'NL91ABNA0417164300', 25.99, true);
DELETE FROM `bank`.`payments`
WHERE `id` = '94f0cbcc-1fce-454e-a6e4-4e3587741808';
COMMIT;
```
```sql [SQLite]
BEGIN;
INSERT INTO "payments" ("id", "account", "amount", "settled") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'NL91ABNA0417164300', 25.99, true);
DELETE FROM "payments"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808';
COMMIT;
```
:::

<CodeWindow title="raw_sql.rs">

```rust
use tank::stream::TryStreamExt;

let rows = connection
    .fetch(
        "SELECT callsign, casualties FROM army.deployments WHERE operational = false",
    )
.try_collect::<Vec<_>>()
.await?;
```

</CodeWindow>

::: code-group
```sql [Postgres]
SELECT "callsign", "casualties"
FROM "army"."deployments"
WHERE "operational" = false;
```
```sql [MySQL]
SELECT `callsign`, `casualties`
FROM `army`.`deployments`
WHERE `operational` = false;
```
```sql [SQLite]
SELECT "callsign", "casualties"
FROM "deployments"
WHERE "operational" = false;
```
:::
