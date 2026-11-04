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
    details: Non-blocking, streaming database operations built for async Rust.
    link: /03-connection
    linkText: Supply lines
  - icon: 🧩
    title: One Entity Model
    details: Derive <code>Entity</code> on a plain struct and get table DDL, selects, inserts and row decoding.
    link: /06-entity-definition
    linkText: Entity definition
  - icon: ⚔️
    title: Precision Fire Control
    details: The <code>expr!</code> macro builds a typed AST driver compiles to its own SQL.
    link: /08-advanced-operations
    linkText: Tactical coordination
  - icon: 🎯
    title: Explicit Joins
    details: Joins are first-class datasets, never hidden behind entity fields. Every alliance is signed.
    link: /08-advanced-operations
    linkText: Joins
  - icon: 🛡️
    title: Adaptable Chassis
    details: Swap SQL and NoSQL backends like changing magazines mid-battle without touching your entity or query code.
    link: /04-capabilities
    linkText: Operational limits
  - icon: 🎖️
    title: Rich Type Arsenal
    details: Map standard Rust types straight to native columns, and forge custom payloads with <code>AsValue</code>.
    link: /05-types
    linkText: Payload specs
---

<script setup>
import SwipeDeck from "./components/SwipeDeck.vue"
import TankJoke from "./components/TankJoke.vue"

const drivers = [
  "Postgres",
  "SQLite",
  "MySQL",
  "MariaDB",
  "DuckDB",
  "MongoDB",
  "ScyllaDB",
  "Cassandra",
  "Valkey",
  "Redis",
]
</script>

Welcome to the armored division, commander. Tank (**T**able **A**bstraction & **N**avigation **K**it) is the Rust data layer built for teams fighting across multiple fronts. Whether you are digging into a local SQLite trench, coordinating a distributed ScyllaDB offensive, or holding a Postgres stronghold, you define your entities once and issue the same explicit orders everywhere. You call the targets, Tank and your driver handle the ballistics.

<dl class="home-stats">
  <div><dt>9</dt><dd>Database backends</dd></div>
  <div><dt>1</dt><dd>Entity model</dd></div>
  <div><dt>0</dt><dd>Boilerplate</dd></div>
  <div><dt>100%</dt><dd>Async</dd></div>
</dl>

<div class="driver-marquee">
  <strong>Supports</strong>
  <div>
    <ul>
      <li v-for="(d, i) in [...drivers, ...drivers]" :key="i">{{ d }}</li>
    </ul>
  </div>
</div>

###### 01 · Unit Schematics
## Define your entity once

Field operations shouldn't require filling out forms in triplicate. Rig a plain Rust struct with `#[derive(Entity)]` and your frontline fighter is combat-ready: field types map to the closest native column each backend supports, `#[tank(...)]` attributes dial in the armor specs, and `#[tank(ignore)]` keeps local recon off the wire.

```rust [schema.rs]
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

Below are sample operations you can issue from Rust and the corresponding SQL executed on each battlefield. Swipe through the deck to cycle maneuvers and inspect the live fire across backends.

<SwipeDeck>
<!--@include: ./components/queries.md-->
</SwipeDeck>

###### 03 · Terrain
## Pick your armor

Not all tanks are built for every terrain, and a lone-wolf SQLite operative has very different mechanical limits from a Postgres artillery battery or a Valkey ammo depot. Every driver implements the same core traits and reports what its engine can pull off: transactions, joins, high-caliber bulk appends, filtering, and aggregations. Consult the [capability matrix](/04-capabilities) before rolling out.

> [!WARNING]
> **No-Fly Zone.** Tank deliberately ships no schema migrations (only table creation and drop for rapid deployment) and no implicit joins. Every alliance is signed in full view — bring a dedicated migration tool for schema evolution and write your joins explicitly.

###### 04 · Deployment
## Skip training, go straight to live fire

Two commands to arm the command vehicle and choose your battlefield, then report to the [getting started briefing](/02-getting-started) to open supply lines, define a unit, and fire for effect. Tank is 100% async, run your campaign under a runtime like [Tokio](https://crates.io/crates/tokio) and `.await` your orders.

```sh
cargo add tank          # the command vehicle
cargo add tank-duckdb   # choose your battlefield
```

###### 05 · Logistics
## Support the Mission
Building and maintaining drivers for several database families is a major effort. **If Tank saves your company time, infrastructure headaches, or boilerplate, please consider supporting its development.**

Sponsorship helps keep Tank maintained, well-tested and moving toward new capabilities faster.

🔗 **[Sponsor the Commander via GitHub Sponsors](https://github.com/sponsors/TankHQ)**. Tank is carefully designed, written and maintained by (human) [barsdeveloper](https://github.com/barsdeveloper).

<TankJoke />
