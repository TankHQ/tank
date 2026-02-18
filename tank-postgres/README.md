<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-postgres

Postgres driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for Postgres, mapping Tank operations and queries into direct Postgres commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

## Features
- Async connection and execution via [`tokio-postgres`](https://crates.io/crates/tokio-postgres)
- TLS via [`postgres-openssl`](https://crates.io/crates/postgres-openssl)
- Fast bulk inserts using the COPY binary protocol

## Install
```sh
cargo add tank
cargo add tank-postgres
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_postgres::PostgresDriver;

let driver = PostgresDriver::new();
let connection = driver
    .connect("postgres://user:pass@hostname:5432/database?sslmode=require&sslrootcert=/path/to/root.crt&sslcert=/path/to/client.crt&sslkey=/path/to/client.key".into())
    .await?;
```

## Running Tests
Tests need a Postgres instance. Provide a connection URL via `TANK_POSTGRES_TEST`. If absent, a containerized Postgres will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
