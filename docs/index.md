---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "TANK"
  text: "Table Abstraction & Navigation Kit"
  tagline: The Rust data layer.
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
    linkText: Connections
  - icon: 🧩
    title: One Entity Model
    details: Derive <code>Entity</code> on a plain struct and get table DDL, selects, inserts and row decoding.
    link: /06-entity-definition
    linkText: Entity definition
  - icon: ⚔️
    title: Typed Expressions
    details: The <code>expr!</code> macro builds a typed AST that each driver compiles to its own SQL.
    link: /08-advanced-operations
    linkText: Advanced operations
  - icon: 🎯
    title: Explicit Joins
    details: Joins are first-class datasets, never hidden behind entity fields. Every alliance is signed.
    link: /08-advanced-operations
    linkText: Joins
  - icon: 🛡️
    title: Adaptable Chassis
    details: Swap SQL and NoSQL backends without touching your entity or query code.
    link: /04-capabilities
    linkText: Feature matrix
  - icon: 🎖️
    title: Rich Type Arsenal
    details: Map ordinary Rust types to native columns, and reach for <code>AsValue</code> for anything custom.
    link: /05-types
    linkText: Types
---

<p class="tk-lead">
Tank (<strong>T</strong>able <strong>A</strong>bstraction &amp; <strong>N</strong>avigation
<strong>K</strong>it) is the Rust data layer that lets you define an entity once and use the same
explicit query API across a wide range of database systems. You define your entities; Tank and
your chosen driver handle the ballistics.
</p>

<HomeStats />

<DriverMarquee />

> [!NOTE]
> In a hurry? The [cheat sheet](/00-cheat-sheet) shows every operation on one page.

<SectionHeading eyebrow="01 · Entity" title="Define your entity once">
A plain Rust struct, one derive. Field types map to the closest column type each backend
supports, <code>#[tank(...)]</code> attributes configure the table, and ignored fields never
reach the database.
</SectionHeading>

<CodeWindow title="schema.rs">

```rust
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

</CodeWindow>

<SectionHeading eyebrow="02 · Operations" title="Every statement, one typed API">
The Rust source lives on the cards, the generated SQL lives in the window beside them. Drag a
card to throw it — the top card follows your hand, gathers momentum and flies away, then the
next one steps up and the window updates.
</SectionHeading>

<SwipeDeck src="/tank/queries.json" />

<SectionHeading eyebrow="03 · Terrain" title="Pick your armor">
Every driver implements the same traits and advertises exactly what its engine can do —
transactions, joins, bulk append, filtering and aggregations. See the
[capability matrix](/04-capabilities) to choose the right backend for the mission.
</SectionHeading>

> [!WARNING]
> **No-Fly Zone.** Tank deliberately ships no schema migrations (only table creation and
> drop for fast setup) and no implicit joins. If you need either, reach for a dedicated
> migration tool and write joins explicitly.

<SectionHeading eyebrow="04 · Deployment" title="Zero setup">
Two commands to arm the vehicle and choose a battlefield, then follow the
[getting started guide](/02-getting-started) to connect, define an entity and fire your first
query. Tank is async: run the examples under a runtime like
[Tokio](https://crates.io/crates/tokio) and <code>.await</code> your operations.
</SectionHeading>

```sh
cargo add tank          # the command vehicle
cargo add tank-duckdb   # choose your battlefield
```

<SectionHeading eyebrow="05 · Support" title="Support the mission">
Building and maintaining drivers for several database families is a major effort. If Tank
saves your company time, infrastructure headaches, or boilerplate, please consider
[supporting its development](https://github.com/sponsors/TankHQ). Tank is carefully designed,
written and maintained by (human) [barsdeveloper](https://github.com/barsdeveloper).
</SectionHeading>

<script setup>
  import TankJoke from "./components/TankJoke.vue"
</script>

<TankJoke />
