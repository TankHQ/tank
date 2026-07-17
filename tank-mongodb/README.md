<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-mongodb

`tank-mongodb` is the MongoDB driver for [Tank](https://crates.io/crates/tank): the Rust data layer.

It maps Tank operations and queries to native MongoDB commands. Use it with the main [`tank`](https://crates.io/crates/tank) crate, which provides entity definitions and the query API.

📘 **Docs:** https://tankhq.github.io/tank

🖥️ **Repo:** https://github.com/TankHQ/tank

📦 **Crate:** https://crates.io/crates/tank-mongodb

## Features
- Async connection and execution via [`mongodb`](https://crates.io/crates/mongodb)
- TLS support
- BSON to Tank Value mapping

## Install
```sh
cargo add tank
cargo add tank-mongodb
```

## Quick Start
```rust
use tank::{ConnectionPool, Driver, PoolConfig};
use tank_mongodb::MongoDBDriver;

let driver = MongoDBDriver::new();
let pool = driver
    .connect_pool(
        "mongodb://user:password@127.0.0.1:27017/database?authSource=admin&tls=true&tlsCAFile=ca.pem&tlsCertificateKeyFile=client-combined.pem".into(),
        PoolConfig::new(),
    )
    .await?;
let mut connection = pool.get().await?;
```

Run this inside an async function. The returned connection can execute Tank entity operations.

Certificate filenames are resolved relative to the working directory. Use paths appropriate for your deployment.

For mutual TLS, `tlsCertificateKeyFile` should point to a PEM file containing both the client certificate and private key.

## Running Tests
Tests need a MongoDB instance. Provide a connection URL via `TANK_MONGODB_TEST`. If absent, a containerized MongoDB will be launched automatically using [testcontainers-modules](https://crates.io/crates/testcontainers-modules).

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
