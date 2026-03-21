<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-scylladb

ScyllaDB and Cassandra driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for ScyllaDB and Cassandra, mapping Tank operations and queries into direct ScyllaDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- Async connection and execution via [`scylla`](https://crates.io/crates/scylla)
- CQL mapping for Tank operations

## Install
```sh
cargo add tank
cargo add tank-scylladb
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_scylladb::ScyllaDBDriver;

let driver = ScyllaDBDriver::new();
let connection = driver
    .connect("scylladb://127.0.0.1:9042/keyspace".into())
    .await?;
```

## Primary Keys
ScyllaDB/Cassandra primary keys have the shape:

`PRIMARY KEY ((partition_key_cols...), clustering_key_cols...)`

In Tank, you define the primary key order via `#[tank(primary_key = (...))]` and mark clustering columns with `#[tank(clustering_key)]`.

- If no field in the primary key is marked `clustering_key`, then all primary-key fields become the partition key.
- If some fields are marked `clustering_key`, then:
  - fields before the first clustering key become the partition key
  - the first clustering key and anything after it (in the PK tuple order) become clustering keys

Example:
```rust
use tank::Entity;

#[derive(Entity)]
#[tank(primary_key = (sensor_id, date, timestamp))]
pub struct SensorData {
    pub sensor_id: String,
    #[tank(clustering_key)]
    pub date: String,
    #[tank(clustering_key)]
    pub timestamp: i64,
    pub value: f64,
}
```
Generates: `PRIMARY KEY ((sensor_id), date, timestamp)`

## Transactions
ScyllaDB/Cassandra do not provide SQL-style, multi-statement ACID transactions.

Tank transactions on this driver are implemented as **batches**:
- `Connection::begin()` starts a **logged batch**.
- Statements are queued during the transaction and only sent to the server on `Transaction::commit()`.
- `Transaction::rollback()` results in no operation because nothing was sent yet.
- `SELECT` (or any query that returns rows) is not meaningful inside a transaction, and will fail when the batch is committed.

If you need different batch semantics, `ScyllaDBConnection` also exposes `begin_unlogged_batch()` and `begin_counter_batch()`.

## Limitations
- **Batches are not ACID transactions**: No interactive reads inside the transaction moreover they are atomic only withing the partition.
- **No `CLUSTERING ORDER BY` in DDL**: `CREATE TABLE` does not currently emit `WITH CLUSTERING ORDER BY ...`.
- No JOIN support: Tank queries requiring joins cannot be executed with this driver.
- `RowsAffected` is not available: The ScyllaDB driver does not report affected-row counts.
- CQL query rules still apply: e.g., `ORDER BY` is only valid on clustering keys and typically requires an equality-restricted partition key.

## Running Tests
Tests need a ScyllaDB instance. Provide a connection URL via `TANK_SCYLLADB_TEST`. If absent, a containerized ScyllaDB will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

1. Ensure Docker is running (linux):
```sh
systemctl status docker
```
2. Add your user to the `docker` group if needed (linux):
```sh
sudo usermod -aG docker $USER
```

> [!CAUTION]
> Avoid aborting tests mid‑run (e.g. killing the process at a breakpoint). Containers might be left running and consume resources.
>
> List containers:
> ```sh
> docker ps
> ```
> Stop container:
> ```sh
> docker kill <container_id_or_name>
> ```
