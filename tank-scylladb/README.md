<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-scylladb

ScyllaDB and Cassandra driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for ScyllaDB, mapping Tank operations and queries into direct ScyllaDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

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
