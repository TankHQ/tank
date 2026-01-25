# Types
###### *Field Manual Section 4* - Payload Specs

Tank brings a full type arsenal to the field. The `Entity` derive macro identifies the type you're using by inspecting its final path segment (the "trailer"). For example, `std::collections::VecDeque`, `collections::VecDeque`, or simply `VecDeque` all resolve to the same list type.

Tank maps ordinary Rust types (numbers, strings, times, collections) to the closest column types each driver supports, falling back to generic representations when appropriate. Below is the standard mapping of Rust types to each driver's column type. `:x:` indicates no native support at this time. Collection types may be emulated in some drivers using generic JSON/text representations.

## Column Types
| Rust                       | Postgres       | SQLite    | MySQL/MariaDB             | DuckDB         | MongoDB     | ScyllaDB/Cassandra |
| -------------------------- | -------------- | --------- | ------------------------- | -------------- | ---------   | ------------------ |
| `bool`                     | `BOOLEAN`      | `INTEGER` | `BOOLEAN`                 | `BOOLEAN`      | `Boolean`   | `BOOLEAN`          |
| `i8`                       | `SMALLINT`     | `INTEGER` | `TINYINT`                 | `TINYINT`      | `Int32`     | `TINYINT`          |
| `i16`                      | `SMALLINT`     | `INTEGER` | `SMALLINT`                | `SMALLINT`     | `Int32`     | `SMALLINT`         |
| `i32`                      | `INTEGER`      | `INTEGER` | `INTEGER`                 | `INTEGER`      | `Int32`     | `INT`              |
| `i64`                      | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`       | `Int64`     | `BIGINT`           |
| `i128`                     | ❌             | ❌        | `NUMERIC(39)`             | `HUGEINT`      | ❌          | `VARINT`           |
| `u8`                       | `SMALLINT`     | `INTEGER` | `TINYINT UNSIGNED`        | `UTINYINT`     | `Int32`     | `SMALLINT`         |
| `u16`                      | `INTEGER`      | `INTEGER` | `SMALLINT UNSIGNED`       | `USMALLINT`    | `Int32`     | `INT`              |
| `u32`                      | `BIGINT`       | `INTEGER` | `INTEGER UNSIGNED`        | `UINTEGER`     | `Int64`     | `BIGINT`           |
| `u64`                      | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`     | `VARINT`           |
| `u128`                     | ❌             | ❌        | `NUMERIC(39) UNSIGNED`    | `UHUGEINT`     | ❌          | `VARINT`           |
| `isize`                    | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`       | `Int64`     | `BIGINT`           |
| `usize`                    | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`     | `VARINT`           |
| `f32`                      | `REAL`         | `REAL`    | `FLOAT`                   | `FLOAT`        | `Double`    | `FLOAT`            |
| `f64`                      | `DOUBLE`       | `REAL`    | `DOUBLE`                  | `DOUBLE`       | `Double`    | `DOUBLE`           |
| `rust_decimal::Decimal`    | `NUMERIC`      | `REAL`    | `DECIMAL`                 | `DECIMAL`      | `Double`    | `DECIMAL`          |
| `tank::FixedDecimal<W, S>` | `NUMERIC(W,S)` | `REAL`    | `DECIMAL(W,S)`            | `DECIMAL(W,S)` | `Double`    | `DECIMAL`          |
| `char`                     | `CHAR(1)`      | `TEXT`    | `CHAR(1)`                 | `CHAR(1)`      | `String`    | `ASCII`            |
| `String`                   | `TEXT`         | `TEXT`    | `TEXT, VARCHAR(60) if pk` | `TEXT`         | `String`    | `TEXT`             |
| `Box<[u8]>`                | `BYTEA`        | `BLOB`    | `BLOB`                    | `BLOB`         | `Binary`    | `BLOB`             |
| `time::Date`               | `DATE`         | `TEXT` ⚠️ | `DATE`                    | `DATE`         | `Date`      | `DATE`             |
| `time::Time`               | `TIME`         | `TEXT` ⚠️ | `TIME(6)`                 | `TIME`         | `String` ⚠️ | `TIME`             |
| `time::PrimitiveDateTime`  | `TIMESTAMP`    | `TEXT` ⚠️ | `DATETIME`                | `TIMESTAMP`    | `DateTime`  | `TIMESTAMP`        |
| `time::OffsetDateTime`     | `TIMESTAMPTZ`  | `TEXT` ⚠️ | `DATETIME`                | `TIMESTAMPTZ`  | `DateTime`  | `TIMESTAMP`        |
| `std::time::Duration`      | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         |
| `time::Duration`           | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         |
| `tank::Interval`           | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         |
| `uuid::Uuid`               | `UUID`         | `TEXT`    | `CHAR(36)`                | `UUID`         | `Uuid`      | `UUID`             |
| `[T; N]`                   | `T[N]`         | ❌        | `JSON` ⚠️                 | `T[N]`         | `Array`     | `VECTOR<T,N>`      |
| `VecDeque<T>`              | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          |
| `LinkedList<T>`            | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          |
| `Vec<T>`                   | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          |
| `HashMap<K, V>`            | ❌             | ❌        | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`  | `MAP<K,V>`         |
| `BTreeMap<K, V>`           | ❌             | ❌        | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`  | `MAP<K,V>`         |

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
