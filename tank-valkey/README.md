<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-valkey

Valkey and Redis driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for Valkey and Redis, mapping Tank operations and queries into direct commands using the `redis` crate. It supports both Valkey and Redis instances.

It does not replace the main [`tank`](https://crates.io/crates/tank) crate. You still need it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Features
- Async connection and execution via [`redis`](https://crates.io/crates/redis)
- Key-Value mapping for Tank entities
- Support for nested types (Vec, HashMap) stored in auxiliary keys

## Limitations
Due to the Key-Value nature of Valkey/Redis, this driver has strict limitations compared to SQL drivers:

- **Primary key access only**: `SELECT` and `DELETE` operations must provide a `WHERE` clause that matches the entity primary key exactly (eg: `WHERE first_name == "Linus" && last_name == "Torvalds"`). Any other type of expressions are not supported.
- **No joins or aggregations**: `JOIN` clauses and `GROUP BY` aggregations are not supported.
- **No ordering**: `ORDER BY` clauses are not supported.
- **No table/schema DDL**: `create_table`/`create_schema` are effectively no-ops; `drop_table` logs an error. Data must be cleared by deleting keys (or via expiration) separately.
- **Data modeling is key-based**: Each entity instance is stored under a single “root” key (a Redis Hash). Nested collections are stored under additional child keys derived from the root key (e.g. `<root>:<field>`).
- **No reliable rows-affected**: Valkey/Redis does not provide affected-row counts in a SQL sense; Tank returns `rows_affected: None`.
- **Transactions are pipelined, not SQL/ACID**: Tank transactions on this driver queue commands and execute them via a Redis pipeline on `commit()`. `rollback()` is a no-op. This is not automatically wrapped in `MULTI/EXEC`.

## Install
```sh
cargo add tank
cargo add tank-valkey
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_valkey::ValkeyDriver;

let driver = ValkeyDriver::default();
let connection = driver
    .connect("redis://127.0.0.1:6379/".into())
    .await?;
```

The driver accepts `valkey://` / `redis://` for plaintext and `valkeys://` / `rediss://` for TLS.

## Running Tests
Tests need a Valkey/Redis instance. Provide a connection URL via `TANK_VALKEY_TEST`. If absent, a containerized Valkey will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

1. Ensure Docker is running (linux):
```sh
systemctl status docker
```
2. Add your user to the `docker` group if needed (linux):
```sh
sudo usermod -aG docker $USER
```

> [!CAUTION]
> Avoid aborting tests mid-run (e.g. killing the process at a breakpoint). Containers might be left running and consume resources.
>
> List containers:
> ```sh
> docker ps
> ```
> Stop container:
> ```sh
> docker kill <container_id_or_name>
> ```
