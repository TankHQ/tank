<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank logo: a circular gold emblem with a military tank and a database symbol" />
</div>

# tank-yourdb

Template driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Use this crate as a starting point when implementing a new driver for Tank.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank

## Checklist
1. Rename `tank-yourdb` to `tank-backendname`
2. Implement `Driver` trait
3. Implement `Connection` and `Executor` traits
4. Implement `SqlWriter` for the dialect
