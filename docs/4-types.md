# Types
###### *Field Manual Section 4* - Payload Specs

Tank brings a full type arsenal to the field. The `Entity` derive macro identifies the type you're using by inspecting its final path segment (the "trailer"). For example, `std::collections::VecDeque`, `collections::VecDeque`, or simply `VecDeque` all resolve to the same list type.

Tank maps ordinary Rust types (numbers, strings, times, collections) to the closest column types each driver supports, falling back to generic representations when appropriate. Below is the standard mapping of Rust types to each driver's column type. `:x:` indicates no native support at this time. Collection types may be emulated in some drivers using generic JSON/text representations.

## Column Types
| Rust                       | DuckDB         | SQLite    | Postgres       | MySQL/MariaDB             | ScyllaDB/Cassandra | 
| -------------------------- | -------------- | --------- | -------------- | ------------------------- | ------------------ |
| `bool`                     | `BOOLEAN`      | `INTEGER` | `BOOLEAN`      | `BOOLEAN`                 | `BOOLEAN`          |
| `i8`                       | `TINYINT`      | `INTEGER` | `SMALLINT`     | `TINYINT`                 | `TINYINT`          |
| `i16`                      | `SMALLINT`     | `INTEGER` | `SMALLINT`     | `SMALLINT`                | `SMALLINT`         |
| `i32`                      | `INTEGER`      | `INTEGER` | `INTEGER`      | `INTEGER`                 | `INT`              |
| `i64`                      | `BIGINT`       | `INTEGER` | `BIGINT`       | `BIGINT`                  | `BIGINT`           |
| `i128`                     | `HUGEINT`      | ❌        | ❌             | `NUMERIC(39)`             | `VARINT`           |
| `u8`                       | `UTINYINT`     | `INTEGER` | `SMALLINT`     | `TINYINT UNSIGNED`        | `SMALLINT`         |
| `u16`                      | `USMALLINT`    | `INTEGER` | `INTEGER`      | `SMALLINT UNSIGNED`       | `INT`              |
| `u32`                      | `UINTEGER`     | `INTEGER` | `BIGINT`       | `INTEGER UNSIGNED`        | `BIGINT`           |
| `u64`                      | `UBIGINT`      | `INTEGER` | `NUMERIC(19)`  | `BIGINT UNSIGNED`         | `VARINT`           |
| `u128`                     | `UHUGEINT`     | ❌        | ❌             | `NUMERIC(39) UNSIGNED`    | `VARINT`           |
| `isize`                    | `BIGINT`       | `INTEGER` | `BIGINT`       | `BIGINT`                  | `BIGINT`           |
| `usize`                    | `UBIGINT`      | `INTEGER` | `NUMERIC(19)`  | `BIGINT UNSIGNED`         | `VARINT`           |
| `f32`                      | `FLOAT`        | `REAL`    | `REAL`         | `FLOAT`                   | `FLOAT`            |
| `f64`                      | `DOUBLE`       | `REAL`    | `DOUBLE`       | `DOUBLE`                  | `DOUBLE`           |
| `rust_decimal::Decimal`    | `DECIMAL`      | `REAL`    | `NUMERIC`      | `DECIMAL`                 | `DECIMAL`          |
| `tank::FixedDecimal<W, S>` | `DECIMAL(W,S)` | `REAL`    | `NUMERIC(W,S)` | `DECIMAL(W,S)`            | `DECIMAL`          |
| `char`                     | `CHAR(1)`      | `TEXT`    | `CHAR(1)`      | `CHAR(1)`                 | `ASCII`            |
| `String`                   | `TEXT`         | `TEXT`    | `TEXT`         | `TEXT, VARCHAR(60) if pk` | `TEXT`             |
| `Box<[u8]>`                | `BLOB`         | `BLOB`    | `BYTEA`        | `BLOB`                    | `BLOB`             |
| `time::Date`               | `DATE`         | `TEXT` ⚠️ | `DATE`         | `DATE`                    | `DATE`             |
| `time::Time`               | `TIME`         | `TEXT` ⚠️ | `TIME`         | `TIME(6)`                 | `TIME`             |
| `time::PrimitiveDateTime`  | `TIMESTAMP`    | `TEXT` ⚠️ | `TIMESTAMP`    | `DATETIME`                | `TIMESTAMP`        |
| `time::OffsetDateTime`     | `TIMESTAMPTZ`  | `TEXT` ⚠️ | `TIMESTAMPTZ`  | `DATETIME`                | `TIMESTAMP`        |
| `std::time::Duration`      | `INTERVAL`     | ❌        | `INTERVAL`     | `TIME(6)`                 | `DURATION`         |
| `time::Duration`           | `INTERVAL`     | ❌        | `INTERVAL`     | `TIME(6)`                 | `DURATION`         |
| `tank::Interval`           | `INTERVAL`     | ❌        | `INTERVAL`     | `TIME(6)`                 | `DURATION`         |
| `uuid::Uuid`               | `UUID`         | `TEXT`    | `UUID`         | `CHAR(36)`                | `UUID`             |
| `[T; N]`                   | `T[N]`         | ❌        | `T[N]`         | `JSON` ⚠️                 | `VECTOR<T,N>`      |
| `VecDeque<T>`              | `T[]`          | ❌        | `T[]`          | `JSON` ⚠️                 | `LIST<T>`          |
| `LinkedList<T>`            | `T[]`          | ❌        | `T[]`          | `JSON` ⚠️                 | `LIST<T>`          |
| `Vec<T>`                   | `T[]`          | ❌        | `T[]`          | `JSON` ⚠️                 | `LIST<T>`          |
| `HashMap<K, V>`            | `MAP(K,V)`     | ❌        | ❌             | `JSON` ⚠️                 | `MAP<K,V>`         |
| `BTreeMap<K, V>`           | `MAP(K,V)`     | ❌        | ❌             | `JSON` ⚠️                 | `MAP<K,V>`         |

> [!WARNING]
> When a type falls back to a generic representation (e.g. `TEXT` or `JSON`), Tank encodes it predictably so equality / ordering comparisons (where meaningful) behave as expected. Advanced indexing or operator support may vary by driver.
>
> The special `isize` / `usize` types map to the native pointer-width integer (64‑bit on 64‑bit targets, 32‑bit on 32‑bit targets). For cross‑database portability prefer explicit `i64` / `u64` unless you truly need platform width.

## Wrapper Types
Built‑in wrappers you can use directly in entities. SQL type is inferred from the inner type.

Supported wrappers:
- `tank::Passive<T>`: Omit on update / allow default generation on insert.
- `Option<T>`: Nullable column.
- `Box<T>`
- `Cell<T>`
- `RefCell<T>`
- `RwLock<T>`
- `Arc<T>`
- `Rc<T>`

*With this arsenal, your entities hit every target, every time.*
