<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-talkey

Valkey and Redis driver implementation for [Tank](https://crates.io/crates/tank): the Rust data layer.

Implements Tank’s `Driver` and related traits for Valkey and Redis, mapping Tank operations and queries into direct DuckDB commands. It does not replace the main [`tank`](https://crates.io/crates/tank) crate. you still use it to define entities, manage schemas, and build queries.

📘 https://tankhq.github.io/tank

🖥️ https://github.com/TankHQ/tank

📦 https://crates.io/crates/tank
