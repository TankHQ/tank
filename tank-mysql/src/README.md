<div align="center">
    <img width="300" height="300" src="../../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-mysql

`tank-mysql` is the MySQL and MariaDB driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native MySQL and MariaDB commands. Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-mysql

## Features
- Async connection and execution via [`mysql_async`](https://crates.io/crates/mysql_async)
- TLS support via `native-tls`
- Support for MariaDB and MySQL

## Install
```sh
cargo add tank
cargo add tank-mysql
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_mysql::MySQLDriver;

let driver = MySQLDriver::new();
let pool = driver
    .connect_pool(
        "mysql://user:password@127.0.0.1:3306/database?require_ssl=true&ssl_ca=ca.pem&ssl_cert=client.p12&ssl_pass=certificate-password".into(),
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.

Certificate filenames are resolved relative to the working directory. Use paths appropriate for your deployment.

## Running Tests
Tests cover both MySQL and MariaDB. Provide connection URLs through `TANK_MYSQL_TEST` and `TANK_MARIADB_TEST`. If either variable is absent, the corresponding containerized database will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
