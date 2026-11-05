# Types
###### *Field Manual Section 5* - Payload Specs

Tank brings a full type arsenal to the field. The `Entity` derive macro identifies known types by matching trailing path segments. For example, `std::collections::VecDeque`, `collections::VecDeque`, or simply `VecDeque` all resolve to the same type: `Value::List`.

Tank maps ordinary Rust types to the closest column types each driver supports, falling back to generic representations when appropriate. The table shows column/storage mappings, not a guarantee of every operation or parameter binding. ❌ indicates unsupported mappings; ⚠️ marks fallback encodings or important limitations.

## Column Types
<div class="sticky-table">

| Rust                      | Postgres       | SQLite    | MySQL                     | MariaDB                   | DuckDB         | MongoDB      | ScyllaDB/Cassandra | Valkey/Redis |
| ------------------------- | -------------- | --------- | ------------------------- | ------------------------- | -------------- | ------------ | ------------------ | ------------ |
| `bool`                    | `BOOLEAN`      | `INTEGER` | `BOOLEAN`                 | `BOOLEAN`                 | `BOOLEAN`      | `Boolean`    | `BOOLEAN`          | `String`     |
| `i8`, `NonZeroI8`         | `SMALLINT`     | `INTEGER` | `TINYINT`                 | `TINYINT`                 | `TINYINT`      | `Int32`      | `TINYINT`          | `String`     |
| `i16`, `NonZeroI16`       | `SMALLINT`     | `INTEGER` | `SMALLINT`                | `SMALLINT`                | `SMALLINT`     | `Int32`      | `SMALLINT`         | `String`     |
| `i32`, `NonZeroI32`       | `INTEGER`      | `INTEGER` | `INTEGER`                 | `INTEGER`                 | `INTEGER`      | `Int32`      | `INT`              | `String`     |
| `i64`, `NonZeroI64`       | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`                  | `BIGINT`       | `Int64`      | `BIGINT`           | `String`     |
| `i128`, `NonZeroI128`     | `NUMERIC(39)`  | ❌        | `NUMERIC(39)`             | `NUMERIC(39)`             | `HUGEINT`      | ❌           | `VARINT`           | `String`     |
| `u8`, `NonZeroU8`         | `SMALLINT`     | `INTEGER` | `TINYINT UNSIGNED`        | `TINYINT UNSIGNED`        | `UTINYINT`     | `Int32`      | `SMALLINT`         | `String`     |
| `u16`, `NonZeroU16`       | `INTEGER`      | `INTEGER` | `SMALLINT UNSIGNED`       | `SMALLINT UNSIGNED`       | `USMALLINT`    | `Int32`      | `INT`              | `String`     |
| `u32`, `NonZeroU32`       | `BIGINT`       | `INTEGER` | `INTEGER UNSIGNED`        | `INTEGER UNSIGNED`        | `UINTEGER`     | `Int64`      | `BIGINT`           | `String`     |
| `u64`, `NonZeroU64`       | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`      | `VARINT`           | `String`     |
| `u128`, `NonZeroU128`     | `NUMERIC(39)`  | ❌        | `NUMERIC(39) UNSIGNED`    | `NUMERIC(39) UNSIGNED`    | `UHUGEINT`     | ❌           | `VARINT`           | `String`     |
| `isize`, `NonZeroIsize`   | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`                  | `BIGINT`       | `Int64`      | `BIGINT`           | `String`     |
| `usize`, `NonZeroUsize`   | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`      | `VARINT`           | `String`     |
| `f32`                     | `REAL`         | `REAL`    | `FLOAT`                   | `FLOAT`                   | `FLOAT`        | `Double`     | `FLOAT`            | `String`     |
| `f64`                     | `FLOAT8`       | `REAL`    | `DOUBLE`                  | `DOUBLE`                  | `DOUBLE`       | `Double`     | `DOUBLE`           | `String`     |
| `rust_decimal::Decimal`   | `NUMERIC`      | `REAL`    | `DECIMAL`                 | `DECIMAL`                 | `DECIMAL`      | `Decimal128` | `DECIMAL`          | `String`     |
| `tank::FixedDecimal<W,S>` | `NUMERIC(W,S)` | `REAL`    | `DECIMAL(W,S)`            | `DECIMAL(W,S)`            | `DECIMAL(W,S)` | `Decimal128` | `DECIMAL`          | `String`     |
| `char`                    | `CHAR(1)`      | `TEXT`    | `CHAR(1)`                 | `CHAR(1)`                 | `CHAR(1)`      | `String`     | `ASCII`            | `String`     |
| `String`                  | `TEXT`         | `TEXT`    | `TEXT, VARCHAR(60) if pk` | `TEXT, VARCHAR(60) if pk` | `TEXT`         | `String`     | `TEXT`             | `String`     |
| `Box<[u8]>`               | `BYTEA`        | `BLOB`    | `BLOB`                    | `BLOB`                    | `BLOB`         | `Binary`     | `BLOB`             | `String` ⚠️  |
| `time::Date`              | `DATE`         | `TEXT` ⚠️ | `DATE`                    | `DATE`                    | `DATE`         | `DateTime`   | `DATE`             | `String`     |
| `time::Time`              | `TIME`         | `TEXT` ⚠️ | `TIME(6)`                 | `TIME(6)`                 | `TIME`         | `String` ⚠️  | `TIME`             | `String`     |
| `time::PrimitiveDateTime` | `TIMESTAMP`    | `TEXT` ⚠️ | `DATETIME`                | `DATETIME`                | `TIMESTAMP`    | `DateTime`   | `TIMESTAMP`        | `String`     |
| `time::UtcDateTime`       | `TIMESTAMP`    | `TEXT` ⚠️ | `DATETIME`                | `DATETIME`                | `TIMESTAMP`    | `DateTime`   | `TIMESTAMP`        | `String`     |
| `time::OffsetDateTime`    | `TIMESTAMPTZ`  | `TEXT` ⚠️ | `DATETIME`                | `DATETIME`                | `TIMESTAMPTZ`  | `DateTime`   | `TIMESTAMP`        | `String`     |
| `std::time::Duration`     | `INTERVAL`     | ❌        | `TIME(6)`                 | `TIME(6)`                 | `INTERVAL`     | ❌           | `DURATION`         | ❌           |
| `time::Duration`          | `INTERVAL`     | ❌        | `TIME(6)`                 | `TIME(6)`                 | `INTERVAL`     | ❌           | `DURATION`         | ❌           |
| `tank::Interval`          | `INTERVAL`     | ❌        | `TIME(6)`                 | `TIME(6)`                 | `INTERVAL`     | ❌           | `DURATION`         | ❌           |
| `uuid::Uuid`              | `UUID`         | `TEXT`    | `CHAR(36)`                | `UUID`                    | `UUID`         | `Uuid`       | `UUID`             | `String`     |
| `[T; N]`                  | `T[N]`         | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `T[N]`         | `Array`      | `VECTOR<T,N>`      | `List`       |
| `VecDeque<T>`             | `T[]`          | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `T[]`          | `Array`      | `LIST<T>`          | `List`       |
| `LinkedList<T>`           | `T[]`          | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `T[]`          | `Array`      | `LIST<T>`          | `List`       |
| `Vec<T>`                  | `T[]`          | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `T[]`          | `Array`      | `LIST<T>`          | `List`       |
| `HashMap<K,V>`            | ❌             | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`   | `MAP<K,V>`         | `Hash`       |
| `BTreeMap<K,V>`           | ❌             | ❌        | `JSON` ⚠️                 | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`   | `MAP<K,V>`         | `Hash`       |
</div>

> [!WARNING]
> Fallback encodings such as `TEXT` or `JSON` have predictable encodings. Usually (but not always) equality and ordering comparisons behave as expected. Advanced indexing or operator support may vary by driver.
>
> Mappings do not imply the full Rust integer range: PostgreSQL's `NUMERIC(19)` cannot hold `u64::MAX`, SQLite and MongoDB limit `u64` to signed 64-bit values, and decimal-based 128-bit conversions have additional limits. The table assumes 64-bit builds for `isize`/`usize`; prefer explicit `i64`/`u64` for portable schemas.

Driver-specific limits:
- MariaDB's native `UUID` requires MariaDB mode and server 10.7+, MySQL mode uses `CHAR(36)`.
- MongoDB stores dates as BSON `DateTime` at UTC midnight. Timestamps use UTC milliseconds, losing original offsets and sub-millisecond precision.
- ScyllaDB maps `[char; N]` to `ASCII`, not `VECTOR<T,N>`. Collection element/key types depend on the backend: Valkey requires scalar elements and map keys/values, and MongoDB requires string-convertible document keys.
- Prepared parameters have additional restrictions: MySQL collections and DuckDB arrays/lists/maps are not supported, and ScyllaDB's `DURATION`, `TIMESTAMP`, and `VARINT` bindings are unimplemented. Valkey blob round trips are not supported reliably, despite Redis strings being binary-safe.

## Wrapper Types
Built-in wrappers usable directly in entities infer the SQL type from the inner type. Persisted fields must also be cloneable:
- `Option<T>`: Nullable column.
- `Box<T>`
- `Cell<T>`
- `RefCell<T>`
- `Arc<T>`
- `Rc<T>`

`RwLock<T>` implements `AsValue` for standalone conversions, but is not directly usable as a persisted entity field because it does not implement `Clone`.

## Custom Types
To handle custom types, implement [`tank::AsValue`](https://docs.rs/tank/latest/tank/trait.AsValue.html). It turns your Rust type into a [`tank::Value`](https://docs.rs/tank/latest/tank/enum.Value.html) for the database and back when decoding rows. With `AsValue` and `Clone`, the type can be used directly as a persisted `Entity` field.

### Example: Custom Struct
Here’s a `host:port` example that encodes a network address as a string that must be stored in a single column:

```rust
use tank::{AsValue, Error, Result, Value, ValueOwned, anyhow};

<!--@include: @/../tank-tests/src/service.rs#host-port-->
```

### Example: Conversion Type
When the custom type lives outside your crate or when you want to change the serialization logic for known types, you can use a conversion wrapper type that implements `AsValue`.

For recognized field types, changing the stored representation may also require a matching `column_type` override. One `AsValue` implementation handles both `MethodWrap` and `MethodWrap::<Option<Method>>`. `TryFrom` rejects SQL `NULL` for required methods, while optional methods preserve it as `None`. The example uses `http::Method`, also re-exported as `reqwest::Method`.

<<< @/../tests/conversion_type.rs#method-conversion

> [!TIP]
> Keep the encoding stable. Your `as_value` output becomes the output format for that field.

*With this arsenal, your entities hit every target, every time.*
