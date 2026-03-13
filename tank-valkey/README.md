<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
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

- **Primary key access only**: `SELECT` and `DELETE` operations must specify a `WHERE` clause that uniquely identifies rows by their PK (eg: `WHERE first_name == "Linus" && last_name == "Torvalds"`). Any other type of expressions are not supported.
- **No joins or aggregations**: `JOIN` clauses and `GROUP BY` aggregations are not supported.
- **No ordering**: `ORDER BY` clauses are not supported.
- **No `DROP TABLE`**: `drop_table` operations are not supported and will log an error. Data must be cleared manually or via key expiration if configured.
- **Data Modeling**: Entities are stored as Hashes. Nested fields (Arrays, Lists, Maps) are stored in separate keys suffixed with the column name (eg: `mytable:1:mycolumn`). This ensures scalar access is fast but requires multiple round-trips for full object retrieval.
- **Transactions**: Uses Redis `MULTI`/`EXEC` blocks, which provide isolation but differ from SQL ACID transactions.

## Install
```sh
cargo add tank
cargo add tank-valkey
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_valkey::ValkeyDriver;

let driver = ValkeyDriver::new();
let connection = driver
    .connect("redis://127.0.0.1:6379/".into())
    .await?;
```

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
