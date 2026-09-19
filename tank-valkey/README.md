<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-valkey

`tank-valkey` is the Valkey and Redis driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native key-value commands through the [`redis`](https://crates.io/crates/redis) crate. It supports both Valkey and Redis servers.

Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-valkey

## Features
- Async connection and execution via [`redis`](https://crates.io/crates/redis)
- Key-Value mapping for Tank entities
- Support for nested types such as `Vec` and `HashMap`, stored in auxiliary keys

## Install
```sh
cargo add tank
cargo add tank-valkey
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_valkey::ValkeyDriver;

let driver = ValkeyDriver::default();
let pool = driver
    .connect_pool(
        "valkeys://user:password@127.0.0.1:6379/0?sslmode=require&sslrootcert=ca.pem&sslcert=client-cert.pem&sslkey=client-key.pem".into(),
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.

Certificate filenames are resolved relative to the working directory. Use paths appropriate for your deployment.

The driver accepts `valkey://` / `redis://` for plaintext and `valkeys://` / `rediss://` for TLS.

## Limitations
Due to the key-value nature of Valkey and Redis, this driver has stricter limitations than SQL drivers:

- **Primary key access only**: `SELECT` and `DELETE` operations must provide a `WHERE` clause that matches the entity primary key exactly, for example `WHERE first_name == "Linus" && last_name == "Torvalds"`. Other expressions are not supported.
- **No joins or aggregations**: `JOIN` clauses and `GROUP BY` aggregations are not supported.
- **No ordering**: `ORDER BY` clauses are not supported.
- **No table or schema DDL**: `create_table` and `create_schema` are effectively no-ops. `drop_table` logs an error. Data must be cleared by deleting keys or through expiration.
- **Data modeling is key-based**: Each entity is stored under one root key as a Redis hash. Nested collections use additional child keys derived from the root key, such as `<root>:<field>`.
- **No reliable rows affected**: Valkey and Redis do not provide affected-row counts in the SQL sense. Tank returns `rows_affected: None`.
- **Transactions use pipelines**: Tank queues commands and executes them through a Redis pipeline on `commit()`. `rollback()` is a no-op. Commands are not automatically wrapped in `MULTI/EXEC`.

## Running Tests
Tests need a Valkey/Redis instance. Provide a connection URL via `TANK_VALKEY_TEST`. If absent, a containerized Valkey will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

1. Ensure Docker is running on Linux:
```sh
systemctl status docker
```
2. Add your user to the `docker` group if needed on Linux:
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
