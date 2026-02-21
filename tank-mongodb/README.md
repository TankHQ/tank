<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-mongodb

MongoDB driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for MongoDB, mapping Tank operations and queries into direct MongoDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

⭐ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- Async connection and execution via [`mongodb`](https://crates.io/crates/mongodb)
- BSON to Tank Value mapping

## Install
```sh
cargo add tank
cargo add tank-mongodb
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_mongodb::MongoDBDriver;

let driver = MongoDBDriver::new();
let connection = driver
    .connect("mongodb://127.0.0.1:27017/database".into())
    .await?;
```

## Running Tests
Tests need a MongoDB instance. Provide a connection URL via `TANK_MONGODB_TEST`. If absent, a containerized MongoDB will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
