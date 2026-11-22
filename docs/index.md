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
      text: Get started
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
    details: Streaming database operations exposed through an async Rust API.
    link: /03-connection
  - icon: 🧩
    title: One Entity Model
    details: Derive <code>Entity</code> on a plain struct and get table DDL, selects, inserts and row decoding.
    link: /06-entity-definition
  - icon: ⚔️
    title: Precision Fire Control
    details: The <code>expr!</code> macro builds a typed AST that each driver translates into its query language.
    link: /08-advanced-operations
  - icon: 🎯
    title: Explicit Joins
    details: Joins are first-class datasets, never hidden behind entity fields. Every alliance is signed.
    link: /08-advanced-operations
  - icon: 🛡️
    title: Adaptable Chassis
    details: Share entity and query code across SQL and NoSQL backends, within each driver's limits.
    link: /04-capabilities
  - icon: 🎖️
    title: Rich Type Arsenal
    details: Map standard Rust types to native columns, and forge custom payloads with <code>AsValue</code>.
    link: /05-types
---

<script setup>
import { withBase } from "vitepress"
import SwipeDeck from "./components/SwipeDeck.vue"
import TankJoke from "./components/TankJoke.vue"

const drivers = [
  { name: "Postgres", section: "postgres" },
  { name: "SQLite", section: "sqlite" },
  { name: "MySQL", section: "mysql-mariadb" },
  { name: "MariaDB", section: "mysql-mariadb" },
  { name: "DuckDB", section: "duckdb" },
  { name: "MongoDB", section: "mongodb" },
  { name: "ScyllaDB", section: "scylladb-cassandra" },
  { name: "Cassandra", section: "scylladb-cassandra" },
  { name: "Valkey", section: "valkey-redis" },
  { name: "Redis", section: "valkey-redis" },
]
</script>

Welcome to the armored division, commander. **Tank (Table Abstraction & Navigation Kit)**: the Rust data layer. Define your entities once and issue the same explicit orders across SQL and NoSQL backends, from a local SQLite file to a Postgres cluster or a distributed ScyllaDB deployment.

<dl class="home-stats">
  <div><dt>10</dt><dd>Database backends</dd></div>
  <div><dt>1</dt><dd>Entity model</dd></div>
  <div><dt>0</dt><dd>Boilerplate</dd></div>
  <div><dt>100%</dt><dd>Async API</dd></div>
</dl>

<div class="driver-marquee">
  <strong>Supports</strong>
  <div>
    <ul>
      <li v-for="(d, i) in [...drivers, ...drivers]" :key="i">
        <a :href="withBase(`/03-connection.html#${d.section}`)">{{ d.name }}</a>
      </li>
    </ul>
  </div>
</div>

###### 01 · Unit Schematics
## Define your entity once

Field operations shouldn't require filling out forms in triplicate. Rig a plain Rust struct with `#[derive(Entity)]` and your frontline fighter is combat-ready: field types map to the closest native column each backend supports, `#[tank(...)]` attributes dial in the armor specs, and `#[tank(ignore)]` keeps local recon off the wire.

```rust [schema.rs]
use std::collections::HashMap;
use tank::Entity;
use uuid::Uuid;

#[derive(Entity)]
#[tank(schema = "army", name = "deployments")]
pub struct Deployment {
    #[tank(primary_key)]
    pub unit_id: Uuid,
    #[tank(name = "callsign")]
    pub callsign: String,
    pub region: String,
    pub casualties: i32,
    pub is_operational: bool,
    #[tank(ignore)]
    pub transient_cache: HashMap<String, String>,
}

#[derive(Entity)]
#[tank(schema = "army", name = "operators")]
pub struct Operator {
    #[tank(primary_key)]
    pub unit_id: Uuid,
    pub name: String,
    pub rank: String,
}
```

###### 02 · Live Fire
## Every maneuver, one typed API

Below are sample Rust operations and representative SQL or native query output for each backend. Each card defines its own entities and uses illustrative UUIDs. Bulk-insert SQL may represent a native ingestion path. Swipe through the deck to cycle maneuvers and inspect the live fire across backends.

<SwipeDeck>

<!--@include: components/queries.md-->
</SwipeDeck>

###### 03 · Terrain
## Pick your armor

Not all tanks are built for every terrain, and a lone-wolf SQLite operative has very different mechanical limits from a Postgres artillery battery or a Valkey ammo depot. Every driver implements the same core traits and reports what its engine can pull off: transactions, joins, high-caliber bulk appends, filtering, and aggregations. Consult the [capability matrix](/04-capabilities) before rolling out.

> [!WARNING]
> **No-Fly Zone.** Tank deliberately ships no schema migrations (only table creation and drop for rapid deployment) and no implicit joins. Every alliance is signed in full view. Bring a dedicated migration tool for schema evolution and write your joins explicitly.

###### 04 · Deployment
## Skip training, go straight to live fire

Two commands to arm the command vehicle and choose your battlefield, then report to the [getting started briefing](/02-getting-started) to open supply lines, define a unit, and fire for effect. Tank exposes an async API: run your campaign under a runtime like [Tokio](https://crates.io/crates/tokio) and `.await` your orders.

```sh
cargo add tank          # the command vehicle
cargo add tank-duckdb   # choose your battlefield
```

###### 05 · Logistics
## Support the Mission

Tank is an independent project maintained by [barsdeveloper](https://github.com/barsdeveloper). Building and testing drivers across several database families takes steady work, and sponsorship directly buys time for new drivers, bug fixes, and performance improvements.

If Tank is useful to you or your team, please consider [sponsoring via GitHub Sponsors](https://github.com/sponsors/TankHQ).

###### 06 · Get a joke

<TankJoke />
