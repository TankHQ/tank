<div align="center">
    <img width="300" height="300" src="../docs/public/logo.png" alt="Tank: Table Abstraction & Navigation Kit logo featuring a green tank with a gear background and stacked database cylinders" />
</div>

# tank-tests

Reusable integration test suite for [Tank](https://crates.io/crates/tank): the Rust data layer.

Instead of duplicating many nearly identical tests inside every driver crate, `tank-tests` centralizes them with feature flags so each driver can opt out gracefully skipping unsupported features.

## Purpose
- Ensures consistency: all drivers pass the same baseline CRUD, query, join, multi‑statement and type coverage.
- Reduces maintenance: add a new scenario once, every driver benefits.
- Accelerates new driver development: run the suite early, disable what you have not implemented yet, iterate.

Each module focuses on a distinct aspect of Tank functionality. They are orchestrated by `execute_tests` in `src/lib.rs`.

## Feature Flags
The crate exposes opt-out feature flags ("disable-*") that skip entire capability families when a driver cannot yet support them.

| Flag                     | Skips tests that use                       |
| ------------------------ | ------------------------------------------ |
| `disable-arrays`         | Fixed-size arrays                          |
| `disable-intervals`      | `Interval` an advanced duration handling   |
| `disable-large-integers` | `i128` and `u128` columns                  |
| `disable-lists`          | List/array-like dynamic collection types   |
| `disable-maps`           | Map containers                             |
| `disable-ordering`       | Explicit result ordering                   |
| `disable-references`     | Referential integrity                      |
| `disable-transactions`   | Transaction begin/commit/rollback coverage |

Use them from the driver crate's `Cargo.toml`:
```toml
[dev-dependencies]
tank-tests = { version = "1", features = ["disable-arrays", "disable-lists", "disable-maps"] }
```

## Setup
1. Add as a dev-dependency and invoke `execute_tests` in a `tokio::test`.
2. Call `init_logs()` to get structured output.
3. Wrap with a mutex to ensure effects do not overlap if the same test is executed twice against the same db.

```rust
#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use tank_core::Driver;
    use tank_mydb::{MyDBConnection, MyDBDriver};
    use tank_tests::{execute_tests, init_logs};
    static MUTEX: Mutex<()> = Mutex::new(());
    #[tokio::test]
    async fn mydb() {
        init_logs();
        let _guard = MUTEX.lock().unwrap();
        let driver = MyDBDriver::new();
        let connection = driver.connect("mydb://localhost:5555".into())
            .await
            .expect("Could not connect to MyDB");
        execute_tests(connection).await; // Runs all enabled modules sequentially
    }
}
```

## Logging Control
- `init_logs()` sets a test-friendly format and default WARN level (override via `RUST_LOG`).
- `silent_logs! { ... }` macro temporarily mutes logging for false positive error logs when they are expected.
