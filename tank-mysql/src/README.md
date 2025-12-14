<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-mysql

MySQL and MariaDB driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for MySQL, mapping Tank operations and queries into direct MySQL commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. you still use it to define entities, manage schemas, and build queries.

https://tankhq.github.io/tank/

https://github.com/TankHQ/tank ⭐

https://crates.io/crates/tank

## Features
- Async connection and execution via [`mysql_async`](https://crates.io/crates/mysql_async)

## Install
```sh
cargo add tank
cargo add tank-mysql
```

## Quick Start
```rust
use tank::{Connection, Driver, Executor};
use tank_mysql::MySQLDriver; // also alias: use tank_mysql::MariaDBDriver;

let driver = MySQLDriver::new();
let connection = driver
    .connect("mysql://tank-mysql-user@localhost:33293/mysql_database?require_ssl=true&ssl_ca=/home/user/Git/tank/tank-mysql/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-mysql/tests/assets/client.p12&ssl_pass=my%26pass%3Fis%3DP%40%24%24".into())
    .await?;
```

## Running Tests
Tests need a Mysql instance and a MariaDB instance. Provide a connection URL via `TANK_MYSQL_TEST` and `TANK_MARIADB_TEST`. If absent, a containerized instance will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
