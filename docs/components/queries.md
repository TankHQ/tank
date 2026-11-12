```rust [create_table.rs]
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
"supply_ratio" NUMERIC);
```
```sql [SQLite]
CREATE TABLE IF NOT EXISTS "army.deployments" (
"unit_id" TEXT PRIMARY KEY,
"callsign" TEXT NOT NULL,
"crew" INTEGER NOT NULL,
"casualties" INTEGER NOT NULL,
"fuel_ratio" REAL NOT NULL,
"operational" INTEGER NOT NULL,
"supply_ratio" REAL);
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
`supply_ratio` DECIMAL);
```
```sql [DuckDB]
CREATE SCHEMA IF NOT EXISTS "army";
CREATE TABLE IF NOT EXISTS "army"."deployments" (
"unit_id" UUID PRIMARY KEY,
"callsign" TEXT NOT NULL,
"crew" UTINYINT NOT NULL,
"casualties" INTEGER NOT NULL,
"fuel_ratio" FLOAT NOT NULL,
"operational" BOOLEAN NOT NULL,
"supply_ratio" DECIMAL);
```
```sql [ScyllaDB]
CREATE KEYSPACE IF NOT EXISTS "army"
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS "army"."deployments" (
"unit_id" UUID PRIMARY KEY,
"callsign" TEXT,
"crew" SMALLINT,
"casualties" INT,
"fuel_ratio" FLOAT,
"operational" BOOLEAN,
"supply_ratio" DECIMAL);
```
:::

```rust [drop_table.rs]
use tank::Entity;

#[derive(Entity)]
#[tank(schema = "army", name = "deployments")]
struct Deployment {}

Deployment::drop_table(&mut connection, true, false).await?;
```

::: code-group
```sql [Postgres]
DROP TABLE IF EXISTS "army"."deployments";
```
```sql [SQLite]
DROP TABLE IF EXISTS "army.deployments";
```
```json [MongoDB]
{
  "drop": "deployments"
}
```
:::

```rust [insert_one.rs]
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

let vehicle = Vehicle {
    id: 1,
    model: "Tiger I".into(),
    ..Default::default()
};
Vehicle::insert_one(&mut connection, &vehicle).await?;
```

::: code-group
```sql [DuckDB]
INSERT INTO "army"."vehicles" ("id", "model", "crew", "weight_kg", "in_service") VALUES
(1, 'Tiger I', 0, 0, false);
```
```sql [SQLite]
INSERT INTO "army.vehicles" ("id", "model", "crew", "weight_kg", "in_service") VALUES
(1, 'Tiger I', 0, 0, false);
```
```sql [MySQL]
INSERT INTO `army`.`vehicles` (`id`, `model`, `crew`, `weight_kg`, `in_service`) VALUES
(1, 'Tiger I', 0, 0, false);
```
```json [MongoDB]
{
  "id": 1,
  "model": "Tiger I",
  "crew": 0,
  "weight_kg": 0,
  "in_service": false
}
```
:::

```rust [insert_many.rs]
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

let rook = Soldier {
    id: Uuid::new_v4(),
    name: "Rook".into(),
    ..Default::default()
};
let viper = Soldier {
    id: Uuid::new_v4(),
    name: "Viper".into(),
    ..Default::default()
};
Soldier::insert_many(&mut connection, &[rook, viper]).await?;
```

::: code-group
```sql [Postgres]
INSERT INTO "army"."soldiers" ("id", "name", "rank", "age") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Rook', '', 0),
('2f4f97da-0278-4c99-bc22-2b3986aeee85', 'Viper', '', 0);
```
```sql [SQLite]
INSERT INTO "army.soldiers" ("id", "name", "rank", "age") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Rook', '', 0),
('2f4f97da-0278-4c99-bc22-2b3986aeee85', 'Viper', '', 0);
```
```json [MongoDB]
[
  {
    "id": {
      "$binary": {
        "base64": "lPDLzB/ORU6m5E41h3QYCA==",
        "subType": "04"
      }
    },
    "name": "Rook",
    "rank": "",
    "age": 0
  },
  {
    "id": {
      "$binary": {
        "base64": "L0+X2gJ4TJm8Iis5hq7uhQ==",
        "subType": "04"
      }
    },
    "name": "Viper",
    "rank": "",
    "age": 0
  }
]
```
```sql [ScyllaDB]
BEGIN BATCH
INSERT INTO "army"."soldiers" ("id", "name", "rank", "age") VALUES
(94f0cbcc-1fce-454e-a6e4-4e3587741808, 'Rook', '', 0);
INSERT INTO "army"."soldiers" ("id", "name", "rank", "age") VALUES
(2f4f97da-0278-4c99-bc22-2b3986aeee85, 'Viper', '', 0);
APPLY BATCH;
```
:::

```rust [save.rs]
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

let mut mission = Mission {
    id: Uuid::new_v4(),
    code_name: "Red Dawn".into(),
    ..Default::default()
};
// Insert, or update when the PK already exists.
mission.progress = 0.4;
mission.save(&mut connection).await?;
```

::: code-group
```sql [Postgres]
INSERT INTO "army"."missions" ("id", "code_name", "progress", "complete") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Red Dawn', 0.4, false)
ON CONFLICT ("id") DO UPDATE SET
"code_name" = EXCLUDED."code_name",
"progress" = EXCLUDED."progress",
"complete" = EXCLUDED."complete";
```
```sql [MariaDB]
INSERT INTO `army`.`missions` (`id`, `code_name`, `progress`, `complete`) VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', 'Red Dawn', 0.4, false)
ON DUPLICATE KEY UPDATE`code_name` = VALUES(`code_name`),
`progress` = VALUES(`progress`),
`complete` = VALUES(`complete`);
```
```json [MongoDB]
{
  "filter": {
    "id": {
      "$binary": {
        "base64": "lPDLzB/ORU6m5E41h3QYCA==",
        "subType": "04"
      }
    }
  },
  "update": {
    "$set": {
      "id": {
        "$binary": {
          "base64": "lPDLzB/ORU6m5E41h3QYCA==",
          "subType": "04"
        }
      },
      "code_name": "Red Dawn",
      "progress": 0.4000000059604645,
      "complete": false
    }
  },
  "upsert": true
}
```
```sql [ScyllaDB]
INSERT INTO "army"."missions" ("id", "code_name", "progress", "complete") VALUES
(94f0cbcc-1fce-454e-a6e4-4e3587741808, 'Red Dawn', 0.4, false);
```
:::

```rust [delete_many.rs]
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
).await?;
```

::: code-group
```sql [Postgres]
DELETE FROM "army"."signals"
WHERE "encrypted" = false;
```
```sql [SQLite]
DELETE FROM "army.signals"
WHERE "encrypted" = false;
```
```sql [MySQL]
DELETE FROM `army`.`signals`
WHERE `encrypted` = false;
```
:::

```rust [find_one.rs]
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

::: code-group
```sql [DuckDB]
SELECT "id", "callsign", "rank"
FROM "army"."operators"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808'
LIMIT 1;
```
```sql [SQLite]
SELECT "id", "callsign", "rank"
FROM "army.operators"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808'
LIMIT 1;
```
```json [MongoDB]
{
  "filter": {
    "id": {
      "$binary": {
        "base64": "lPDLzB/ORU6m5E41h3QYCA==",
        "subType": "04"
      }
    }
  },
  "projection": {
    "id": 1,
    "callsign": 1,
    "rank": 1
  },
  "limit": 1
}
```
```sql [ScyllaDB]
SELECT "id", "callsign", "rank"
FROM "army"."operators"
WHERE "id" = 94f0cbcc-1fce-454e-a6e4-4e3587741808
LIMIT 1;
```
:::

```rust [find_many.rs]
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

::: code-group
```sql [Postgres]
SELECT "id", "model", "battery_pct", "active"
FROM "army"."drones"
WHERE "active" = true AND "battery_pct" > 20
LIMIT 50;
```
```sql [MariaDB]
SELECT `id`, `model`, `battery_pct`, `active`
FROM `army`.`drones`
WHERE `active` = true AND `battery_pct` > 20
LIMIT 50;
```
```json [MongoDB]
{
  "filter": {
    "$and": [
      {
        "active": true
      },
      {
        "battery_pct": {
          "$gt": 20
        }
      }
    ]
  },
  "projection": {
    "id": 1,
    "model": 1,
    "battery_pct": 1,
    "active": 1
  },
  "limit": 50
}
```
:::

```rust [prepare.rs]
use tank::{Entity, Row, expr, stream::TryStreamExt};
use time::PrimitiveDateTime;
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "messages")]
struct Message {
    #[tank(primary_key)]
    id: Uuid,
    sender: Uuid,
    body: String,
    sent_at: PrimitiveDateTime,
}

let mut query = Message::prepare_find(
    &mut connection,
    expr!(Message::sender == ?),
    Some(50),
).await?;
query.bind(Uuid::new_v4())?;
let rows = connection
    .fetch(&mut query)
    .map_ok(Row::into_owned)
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [Postgres]
SELECT "id", "sender", "body", "sent_at"
FROM "army"."messages"
WHERE "sender" = $1
LIMIT 50;
```
```sql [DuckDB]
SELECT "id", "sender", "body", "sent_at"
FROM "army"."messages"
WHERE "sender" = ?
LIMIT 50;
```
```sql [MySQL]
SELECT `id`, `sender`, `body`, `sent_at`
FROM `army`.`messages`
WHERE `sender` = ?
LIMIT 50;
```
```json [MongoDB]
{
  "filter": {
    "$expr": {
      "$eq": [
        "$sender",
        "$$param_0"
      ]
    }
  },
  "projection": {
    "id": 1,
    "sender": 1,
    "body": 1,
    "sent_at": 1
  },
  "limit": 50
}
```
:::

```rust [expr.rs]
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

::: code-group
```sql [Postgres]
SELECT "id", "name", "country", "value"
FROM "army"."metrics"
WHERE "name" LIKE 'income%' AND "country" IN ('NL','UK') AND "value" > 0.0;
```
```sql [SQLite]
SELECT "id", "name", "country", "value"
FROM "army.metrics"
WHERE "name" LIKE 'income%' AND "country" IN ('NL','UK') AND "value" > 0.0;
```
```sql [MySQL]
SELECT `id`, `name`, `country`, `value`
FROM `army`.`metrics`
WHERE `name` LIKE 'income%' AND `country` IN ('NL','UK') AND `value` > 0.0;
```
```json [MongoDB]
{
  "filter": {
    "$and": [
      {
        "name": {
          "$regex": {
            "$regularExpression": {
              "pattern": "^income.*$",
              "options": ""
            }
          }
        }
      },
      {
        "country": {
          "$in": [
            "NL",
            "UK"
          ]
        }
      },
      {
        "value": {
          "$gt": 0.0
        }
      }
    ]
  },
  "projection": {
    "id": 1,
    "name": 1,
    "country": 1,
    "value": 1
  }
}
```
:::

```rust [query_builder.rs]
use tank::{Entity, Sql, cols, expr, stream::TryStreamExt};

#[derive(Entity)]
#[tank(schema = "army", name = "readings")]
struct Reading {
    #[tank(primary_key)]
    id: i64,
    sensor: String,
    celsius: f32,
    humidity: f32,
}

let hottest = connection
    .fetch(
        Sql
            .select(cols!(Reading::sensor, Reading::celsius))
            .from(Reading::table())
            .where_expr(expr!(Reading::celsius > 30.0))
            .order_by(cols!(Reading::celsius DESC))
            .limit(Some(10))
            .build(&connection.driver()),
    )
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [DuckDB]
SELECT "sensor", "celsius"
FROM "army"."readings"
WHERE "celsius" > 30.0
ORDER BY "celsius" DESC
LIMIT 10;
```
```sql [SQLite]
SELECT "sensor", "celsius"
FROM "army.readings"
WHERE "celsius" > 30.0
ORDER BY "celsius" DESC
LIMIT 10;
```
```sql [MariaDB]
SELECT `sensor`, `celsius`
FROM `army`.`readings`
WHERE `celsius` > 30.0
ORDER BY `celsius` DESC
LIMIT 10;
```
:::

```rust [group_by.rs]
use tank::{Entity, Sql, cols, stream::TryStreamExt};
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

let by_symbol = connection
    .fetch(
        Sql
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
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [Postgres]
SELECT "symbol", COUNT(*) AS "fills", SUM("quantity") AS "volume"
FROM "trading"."trades"
GROUP BY "symbol"
ORDER BY "symbol" ASC;
```
```sql [SQLite]
SELECT "symbol", COUNT(*) AS "fills", SUM("quantity") AS "volume"
FROM "trading.trades"
GROUP BY "symbol"
ORDER BY "symbol" ASC;
```
```sql [MySQL]
SELECT `symbol`, COUNT(*) AS `fills`, SUM(`quantity`) AS `volume`
FROM `trading`.`trades`
GROUP BY `symbol`
ORDER BY `symbol` ASC;
```
```json [MongoDB]
[
  {
    "$group": {
      "_id": {
        "symbol": "$symbol"
      },
      "fills": {
        "$sum": 1
      },
      "volume": {
        "$sum": "$quantity"
      }
    }
  },
  {
    "$sort": {
      "trading._id.symbol": 1
    }
  },
  {
    "$project": {
      "symbol": "$_id.symbol",
      "fills": "$fills",
      "volume": "$volume"
    }
  }
]
```
:::

```rust [having.rs]
use tank::{Entity, Sql, cols, expr, stream::TryStreamExt};

#[derive(Entity)]
#[tank(schema = "ops", name = "events")]
struct Event {
    #[tank(primary_key)]
    id: i64,
    source: String,
    level: String,
    count: u32,
}

let noisy = connection
    .fetch(
        Sql
            .select(cols!(Event::level, SUM(Event::count) as total))
            .from(Event::table())
            .where_expr(expr!(Event::source != "internal"))
            .group_by([Event::level])
            .having(expr!(SUM(Event::count) > 1000))
            .order_by(cols!(Event::level ASC))
            .limit(Some(5))
            .build(&connection.driver()),
    )
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [Postgres]
SELECT "level", SUM("count") AS "total"
FROM "ops"."events"
WHERE "source" != 'internal'
GROUP BY "level"
HAVING SUM("count") > 1000
ORDER BY "level" ASC
LIMIT 5;
```
```sql [SQLite]
SELECT "level", SUM("count") AS "total"
FROM "ops.events"
WHERE "source" != 'internal'
GROUP BY "level"
HAVING SUM("count") > 1000
ORDER BY "level" ASC
LIMIT 5;
```
```sql [MySQL]
SELECT `level`, SUM(`count`) AS `total`
FROM `ops`.`events`
WHERE `source` != 'internal'
GROUP BY `level`
HAVING SUM(`count`) > 1000
ORDER BY `level` ASC
LIMIT 5;
```
```json [MongoDB]
[
  {
    "$match": {
      "source": {
        "$ne": "internal"
      }
    }
  },
  {
    "$group": {
      "_id": {
        "level": "$level"
      },
      "total": {
        "$sum": "$count"
      }
    }
  },
  {
    "$match": {
      "total": {
        "$gt": 1000
      }
    }
  },
  {
    "$sort": {
      "ops._id.level": 1
    }
  },
  {
    "$limit": 5
  },
  {
    "$project": {
      "level": "$_id.level",
      "total": "$total"
    }
  }
]
```
:::

```rust [join.rs]
use tank::{Entity, Sql, cols, join, stream::TryStreamExt};

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
        Sql
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

::: code-group
```sql [Postgres]
SELECT "library"."books"."title", "library"."authors"."name"
FROM "library"."books" JOIN "library"."authors" ON "library"."books"."author_id" = "library"."authors"."id";
```
```sql [SQLite]
SELECT "library.books"."title", "library.authors"."name"
FROM "library.books" JOIN "library.authors" ON "library.books"."author_id" = "library.authors"."id";
```
```sql [MySQL]
SELECT `library`.`books`.`title`, `library`.`authors`.`name`
FROM `library`.`books` JOIN `library`.`authors` ON `library`.`books`.`author_id` = `library`.`authors`.`id`;
```
:::

```rust [left_join.rs]
use tank::{Entity, Sql, cols, expr, join, stream::TryStreamExt};
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
        Sql
            .select(cols!(O.total, C.name as customer))
            .from(join!(
                Order O LEFT JOIN Customer C
                    ON O.customer_id == C.id
            ))
            .where_expr(expr!(O.total > 100.0))
            .order_by(cols!(O.total DESC))
            .build(&connection.driver()),
    )
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [DuckDB]
SELECT O.total, C.name AS "customer"
FROM "shop"."orders" O LEFT JOIN "shop"."customers" C ON O.customer_id = C.id
WHERE O.total > 100.0
ORDER BY O.total DESC;
```
```sql [SQLite]
SELECT O.total, C.name AS "customer"
FROM "shop.orders" O LEFT JOIN "shop.customers" C ON O.customer_id = C.id
WHERE O.total > 100.0
ORDER BY O.total DESC;
```
```sql [MariaDB]
SELECT O.total, C.name AS `customer`
FROM `shop`.`orders` O LEFT JOIN `shop`.`customers` C ON O.customer_id = C.id
WHERE O.total > 100.0
ORDER BY O.total DESC;
```
```text [MongoDB]
The table is not specified in the dataset (if it is a JOIN, MongoDB does not support it)
```
:::

```rust [transaction.rs]
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

let payment = Payment {
    id: Uuid::new_v4(),
    amount: Decimal::new(2599, 2),
    ..Default::default()
};
let mut tx = connection.begin().await?;
Payment::insert_one(&mut tx, &payment).await?;
payment.delete(&mut tx).await?;
tx.commit().await?;
```

::: code-group
```sql [Postgres]
BEGIN;
INSERT INTO "bank"."payments" ("id", "account", "amount", "settled") VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', '', 25.99, false);
DELETE FROM "bank"."payments"
WHERE "id" = '94f0cbcc-1fce-454e-a6e4-4e3587741808';
COMMIT;
```
```sql [MySQL]
BEGIN;
INSERT INTO `bank`.`payments` (`id`, `account`, `amount`, `settled`) VALUES
('94f0cbcc-1fce-454e-a6e4-4e3587741808', '', 25.99, false);
DELETE FROM `bank`.`payments`
WHERE `id` = '94f0cbcc-1fce-454e-a6e4-4e3587741808';
COMMIT;
```
```json [MongoDB]
[
  {
    "id": {
      "$binary": {
        "base64": "lPDLzB/ORU6m5E41h3QYCA==",
        "subType": "04"
      }
    },
    "account": "",
    "amount": {
      "$numberDecimal": "25.99"
    },
    "settled": false
  },
  {
    "id": {
      "$binary": {
        "base64": "lPDLzB/ORU6m5E41h3QYCA==",
        "subType": "04"
      }
    }
  }
]
```
```sql [ScyllaDB]
BEGIN BATCH
INSERT INTO "bank"."payments" ("id", "account", "amount", "settled") VALUES
(94f0cbcc-1fce-454e-a6e4-4e3587741808, '', 25.99, false);
DELETE FROM "bank"."payments"
WHERE "id" = 94f0cbcc-1fce-454e-a6e4-4e3587741808;
APPLY BATCH;
```
:::

```rust [raw_sql.rs]
use indoc::indoc;
use tank::stream::TryStreamExt;

let rows = connection
    .fetch(indoc! {"
        SELECT callsign, casualties
        FROM army.deployments
        WHERE operational = false
    "})
    .try_collect::<Vec<_>>()
    .await?;
```

::: code-group
```sql [Postgres]
SELECT callsign, casualties
FROM army.deployments
WHERE operational = false
```
```text [MongoDB]
Query is not the expected tank::Query::Prepared variant (MongoDB driver uses prepared)
```
:::
