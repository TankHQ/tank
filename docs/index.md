---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "TANK"
  text: "Table Abstraction & Navigation Kit"
  tagline: The Rust data layer
  image:
    src: logo.png
    alt: Tank logo
  actions:
    - theme: brand
      text: Getting started
      link: /02-getting-started
    - theme: alt
      text: Cheat sheet
      link: /00-cheat-sheet
    - theme: alt
      text: View on crates.io
      link: https://crates.io/crates/tank

features:
  - icon: ⚡
    title: Async Firepower
    details: Build on non-blocking database operations designed for async Rust applications.
  - icon: ⚔️
    title: Explicit Fire Control
    details: Use typed expressions and joins, then deploy raw SQL when the abstraction is not enough.
  - icon: 🧩
    title: Adaptable Chassis
    details: Swap database backends seamlessly like changing magazines mid-battle without friction.
  - icon: 🎖️
    title: Rich type arsenal
    details: Convert complex Rust types to database equivalents automatically and safely.
---

<script setup>
  import TankJoke from "./components/TankJoke.vue"
</script>

## Tank is...

A **data layer for Rust**.

First the bad news: Tank is not an actual tank. No armor, no turret. The only thing it
destroys is boilerplate.

Here's the good news. You describe your data as a plain Rust struct and derive `Entity`.
Tank turns that one definition into table setup, inserts, updates, typed queries, joins and transactions. It does all of that through a **driver**, which means the exact same struct works against Postgres, SQLite, MySQL, DuckDB, ClickHouse, MongoDB, ScyllaDB, Valkey, and friends.

```rust
use tank::Entity;

#[derive(Entity)]
#[tank(schema = "army")]
pub struct Tank {
    #[tank(primary_key)]
    pub name: String,
    pub country: String,
    #[tank(name = "caliber")]
    pub caliber_mm: u16,
    pub is_operational: bool,
    pub units_produced: Option<u32>,
}

Tank::create_table(&mut connection, true, true).await?;
my_tank.save(&mut connection).await?;

let operational = Tank::find_many(
    &mut connection,
    expr!(Tank::is_operational == true),
    Some(1000),
).try_collect::<Vec<_>>().await?;
```

Change the driver, keep the code. That's the whole sales pitch. Everything below is just
showing off.

## What Tank doesn't do

Honesty is a virtue, so here's the short list:

- **Migrations.** None. You get table create and drop. Perfect for prototypes, tests and
  the first week of a startup.
- **Implicit joins.** No entities hiding inside entities. If you want a join, you write a
  join, like a grown-up.
- **Take your SQL away.** Abstraction is a convenience, not a hostage situation. Drop to
  raw SQL whenever it suits you.

## One struct, four native tongues

Still not convinced the "any database" thing is real? Watch one definition fan out.

```rust
#[derive(Entity)]
#[tank(schema = "recon")]
pub struct RadarContact {
    #[tank(primary_key)]
    pub id: Uuid,
    pub callsign: String,
    pub bearing: f64,
    pub range_km: u32,
    #[tank(clustering_key)]
    pub spotted_at: OffsetDateTime,
}
```

This single `create_table` call:

```rust
RadarContact::create_table(&mut connection, true, true).await?;
```

...produces four completely different things, depending on who's driving:

::: code-group
```sql [Postgres]
CREATE TABLE IF NOT EXISTS "recon"."radar_contact" (
    "id"          UUID PRIMARY KEY,
    "callsign"    TEXT NOT NULL,
    "bearing"     DOUBLE NOT NULL,
    "range_km"    BIGINT NOT NULL,
    "spotted_at"  TIMESTAMPTZ NOT NULL);
```

```json [MongoDB]
{
  "create": "radar_contact",
  "comment": "Tank: create collection recon.radar_contact",
  "id":           "6f9c2b1a-...",
  "callsign":     "Falcon",
  "bearing":      137.4,
  "range_km":     82,
  "spotted_at":   { "$date": "2026-09-22T14:03:00Z" }
}
```

```sql [ScyllaDB]
CREATE TABLE recon.radar_contact (
    id          uuid,
    callsign    text,
    bearing     double,
    range_km    bigint,
    spotted_at  timestamp,
    PRIMARY KEY ((id), spotted_at)
);
```

```text [Valkey]
HSET recon:radar_contact:6f9c2b1a-...
     callsign Falcon
     bearing  137.4
     range_km 82
     spotted_at 2026-09-22T14:03:00Z
```
:::

A relational table, a JSON document, a partition-and-clustering table, a key-value hash.
One struct, zero rewrites, no database-shaped scars on your architecture.

## And reading is just as portable

```rust
let contacts = RadarContact::find_many(
    &mut connection,
    expr!(RadarContact::callsign == "Falcon" && RadarContact::bearing > 90.0),
    Some(10),
)
.try_collect::<Vec<_>>()
.await?;
```

That one expression becomes `= 'Falcon'` in Postgres, a `{"callsign": "Falcon"}` filter in
MongoDB, and a key lookup in Valkey. Same code, four dialects, nobody got hurt.

Ready to enlist? Head to [Basic training](/02-getting-started), or grab the
[cheat sheet](/00-cheat-sheet) if you just want the API and no jokes.

*Rustaceans don't hide behind ORMs, they drive Tanks.*

<TankJoke />
