<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-mssql

`tank-mssql` is the Microsoft SQL Server / Azure SQL driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native T-SQL commands. Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-mssql

## Features
- Async connection and execution via [`tiberius`](https://crates.io/crates/tiberius)
- TLS support via `rustls`
- Support for Microsoft SQL Server and Azure SQL

## Install
```sh
cargo add tank
cargo add tank-mssql
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_mssql::MSSQLDriver;

let driver = MSSQLDriver::new();
let pool = driver
    .connect_pool(
        "mssql://sa:yourStrong(!)Password@localhost:1433/database?encrypt=true&ssl_ca=ca.pem",
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.

Certificate filenames are resolved relative to the working directory. Use paths appropriate for your deployment.

## Running Tests
Tests need a Microsoft SQL Server instance. Provide a connection URL via `TANK_MSSQL_TEST`. If absent, a containerized SQL Server will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
