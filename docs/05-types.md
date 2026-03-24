# Types
###### *Field Manual Section 5* - Payload Specs

Tank brings a full type arsenal to the field. The `Entity` derive macro identifies the type you're using by inspecting its final path segment (the trailer). For example, `std::collections::VecDeque`, `collections::VecDeque`, or simply `VecDeque` all resolve to the same type: `Value::List`.

Tank maps ordinary Rust types (check the table below) to the closest column types each driver supports, falling back to generic representations when appropriate. Below is the standard mapping of Rust types to each driver's column type. The symbol ❌ indicates no native support at this time. Collection types may be emulated in some drivers using generic JSON or text representations.

## Column Types
<div class="sticky-table">

| Rust                      | Postgres       | SQLite    | MySQL/MariaDB             | DuckDB         | MongoDB     | ScyllaDB/Cassandra | Valkey/Redis |
| ------------------------- | -------------- | --------- | ------------------------- | -------------- | ----------- | ------------------ | ------------ |
| `bool`                    | `BOOLEAN`      | `INTEGER` | `BOOLEAN`                 | `BOOLEAN`      | `Boolean`   | `BOOLEAN`          | `String`     |
| `i8`, `NonZeroI8`         | `SMALLINT`     | `INTEGER` | `TINYINT`                 | `TINYINT`      | `Int32`     | `TINYINT`          | `String`     |
| `i16`, `NonZeroI16`       | `SMALLINT`     | `INTEGER` | `SMALLINT`                | `SMALLINT`     | `Int32`     | `SMALLINT`         | `String`     |
| `i32`, `NonZeroI32`       | `INTEGER`      | `INTEGER` | `INTEGER`                 | `INTEGER`      | `Int32`     | `INT`              | `String`     |
| `i64`, `NonZeroI64`       | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`       | `Int64`     | `BIGINT`           | `String`     |
| `i128`, `NonZeroI128`     | `NUMERIC(39)`  | ❌        | `NUMERIC(39)`             | `HUGEINT`      | ❌          | `VARINT`           | `String`     |
| `u8`, `NonZeroU8`         | `SMALLINT`     | `INTEGER` | `TINYINT UNSIGNED`        | `UTINYINT`     | `Int32`     | `SMALLINT`         | `String`     |
| `u16`, `NonZeroU16`       | `INTEGER`      | `INTEGER` | `SMALLINT UNSIGNED`       | `USMALLINT`    | `Int32`     | `INT`              | `String`     |
| `u32`, `NonZeroU32`       | `BIGINT`       | `INTEGER` | `INTEGER UNSIGNED`        | `UINTEGER`     | `Int64`     | `BIGINT`           | `String`     |
| `u64`, `NonZeroU64`       | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`     | `VARINT`           | `String`     |
| `u128`, `NonZeroU128`     | `NUMERIC(39)`  | ❌        | `NUMERIC(39) UNSIGNED`    | `UHUGEINT`     | ❌          | `VARINT`           | `String`     |
| `isize`, `NonZeroIsize`   | `BIGINT`       | `INTEGER` | `BIGINT`                  | `BIGINT`       | `Int64`     | `BIGINT`           | `String`     |
| `usize`, `NonZeroUsize`   | `NUMERIC(19)`  | `INTEGER` | `BIGINT UNSIGNED`         | `UBIGINT`      | `Int64`     | `VARINT`           | `String`     |
| `f32`                     | `REAL`         | `REAL`    | `FLOAT`                   | `FLOAT`        | `Double`    | `FLOAT`            | `String`     |
| `f64`                     | `DOUBLE`       | `REAL`    | `DOUBLE`                  | `DOUBLE`       | `Double`    | `DOUBLE`           | `String`     |
| `rust_decimal::Decimal`   | `NUMERIC`      | `REAL`    | `DECIMAL`                 | `DECIMAL`      | `Double`    | `DECIMAL`          | `String`     |
| `tank::FixedDecimal<W,S>` | `NUMERIC(W,S)` | `REAL`    | `DECIMAL(W,S)`            | `DECIMAL(W,S)` | `Double`    | `DECIMAL`          | `String`     |
| `char`                    | `CHAR(1)`      | `TEXT`    | `CHAR(1)`                 | `CHAR(1)`      | `String`    | `ASCII`            | `String`     |
| `String`                  | `TEXT`         | `TEXT`    | `TEXT, VARCHAR(60) if pk` | `TEXT`         | `String`    | `TEXT`             | `String`     |
| `Box<[u8]>`               | `BYTEA`        | `BLOB`    | `BLOB`                    | `BLOB`         | `Binary`    | `BLOB`             | `String`     |
| `time::Date`              | `DATE`         | `TEXT` ⚠️ | `DATE`                    | `DATE`         | `Date`      | `DATE`             | `String`     |
| `time::Time`              | `TIME`         | `TEXT` ⚠️ | `TIME(6)`                 | `TIME`         | `String` ⚠️ | `TIME`             | `String`     |
| `time::PrimitiveDateTime` | `TIMESTAMP`    | `TEXT` ⚠️ | `DATETIME`                | `TIMESTAMP`    | `DateTime`  | `TIMESTAMP`        | `String`     |
| `time::UtcDateTime`       | `TIMESTAMP`    | `TEXT` ⚠️ | `DATETIME`                | `TIMESTAMP`    | `DateTime`  | `TIMESTAMP`        | `String`     |
| `time::OffsetDateTime`    | `TIMESTAMPTZ`  | `TEXT` ⚠️ | `DATETIME`                | `TIMESTAMPTZ`  | `DateTime`  | `TIMESTAMP`        | `String`     |
| `std::time::Duration`     | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         | `String`     |
| `time::Duration`          | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         | `String`     |
| `tank::Interval`          | `INTERVAL`     | ❌        | `TIME(6)`                 | `INTERVAL`     | ❌          | `DURATION`         | `String`     |
| `uuid::Uuid`              | `UUID`         | `TEXT`    | `CHAR(36)`                | `UUID`         | `Uuid`      | `UUID`             | `String`     |
| `[T; N]`                  | `T[N]`         | ❌        | `JSON` ⚠️                 | `T[N]`         | `Array`     | `VECTOR<T,N>`      | `List`       |
| `VecDeque<T>`             | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          | `List`       |
| `LinkedList<T>`           | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          | `List`       |
| `Vec<T>`                  | `T[]`          | ❌        | `JSON` ⚠️                 | `T[]`          | `Array`     | `LIST<T>`          | `List`       |
| `HashMap<K,V>`            | ❌             | ❌        | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`  | `MAP<K,V>`         | `Hash`       |
| `BTreeMap<K,V>`           | ❌             | ❌        | `JSON` ⚠️                 | `MAP(K,V)`     | `Document`  | `MAP<K,V>`         | `Hash`       |
</div>

> [!WARNING]
> When a type falls back to a generic representation (like `TEXT` or `JSON`), Tank encodes it predictably such that equality and ordering comparisons (where meaningful) behave as expected. Advanced indexing or operator support may vary by driver.
>
> The special `isize`/`usize` types map to the native pointer-width integer (64-bit on 64-bit targets, 32-bit on 32-bit targets). For cross-database portability prefer explicit `i64`/`u64` unless you truly need platform width.

## Wrapper Types
Built-in wrappers you can use directly in entities, the SQL type is inferred from the inner type:
- `Option<T>`: Nullable column.
- `Box<T>`
- `Cell<T>`
- `RefCell<T>`
- `RwLock<T>`
- `Arc<T>`
- `Rc<T>`

## Custom Types
The handle custom types you just need to implement [`tank::AsValue`](https://docs.rs/tank/latest/tank/trait.AsValue.html). It will be your conversion contract: it turns your Rust type into a [`tank::Value`](https://docs.rs/tank/latest/tank/enum.Value.html) that can be sent to the database, and turns a `tank::Value` back into the original when decoding rows. Once implemented, you can use the type directly as an `Entity` field.

### Example: Custom Struct
Here’s a `host:port` example that encodes a network address as a string that must be stored in a single column:

```rust
use tank::{AsValue, Error, Result, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl AsValue for HostPort {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }

    fn as_value(self) -> Value {
        Value::Varchar(Some(format!("{}:{}", self.host, self.port).into()))
    }

    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        // Always call try_as before checking the received value
        match value.try_as(&Value::Varchar(None)) {
            Ok(Value::Varchar(Some(v))) => {
                let context = || Error::msg(format!("Failed to parse HostPort from value `{v}`"));
                let (host, port) = v.split_once(':').ok_or_else(context)?;
                Ok(Self {
                    host: host.to_string(),
                    port: port
                        .parse::<u16>()
                        .map_err(|e| Error::new(e).context(context()))?,
                })
            }
            _ => Err(Error::msg(
                "Could not convert value into HostPort (expected Value::Varchar)",
            )),
        }
    }
}
```

### Example: Conversion Type
When the custom type lives outside your crate or when you want to change the serialization logic for known types, you can use a conversion wrapper type that implements `AsValue`.

```rust
use anyhow::Context;
use reqwest::Method;
use std::{any, str::FromStr};

#[derive(Clone, PartialEq, Eq, tank::Entity, Debug)]
pub struct Request {
    #[tank(primary_key)]
    pub id: i64,
    pub target: String,
    #[tank(conversion_type = MethodWrap)]
    pub method: Method,
    pub beign_timestamp_ms: i64,
    pub end_timestamp_ms: Option<i64>,
}
#[derive(Clone, PartialEq, Eq, tank::Entity, Debug)]
pub struct RequestLimit {
    #[tank(primary_key)]
    pub id: i64,
    pub target_pattern: String,
    pub requests: u32,
    #[tank(conversion_type = MethodWrap)]
    pub method: Option<Method>,
    pub interval_ms: Option<u32>,
}

// Declare a local wrapper making it possible to implement `tank::AsValue`
pub struct MethodWrap(Option<Method>);
impl tank::AsValue for MethodWrap {
    fn as_empty_value() -> tank::Value {
        tank::Value::Varchar(None)
    }

    fn as_value(self) -> tank::Value {
        self.0.map(|v| v.to_string()).as_value()
    }

    fn try_from_value(value: tank::Value) -> tank::Result<Self>
    where
        Self: Sized,
    {
        if value.is_null() {
            return Ok(Self(None));
        }
        // Always call try_as before checking the received value
        match value.try_as(&tank::Value::Varchar(None)) {
            Ok(tank::Value::Varchar(Some(v), ..)) => {
                let method = Method::from_str(&v).with_context(|| {
                    format!("Could not convert {v:?} into {}", type_name::<Method>())
                })?;

                Ok(method.into())
            }
            _ => Err(tank::Error::msg(format!(
                "Could not convert value into {}",
                type_name::<Method>()
            ))),
        }
    }
}

// Implement conversion logic for each type this method has to convert
impl From<Method> for MethodWrap {
    fn from(value: Method) -> Self {
        Self(Some(value))
    }
}
impl From<MethodWrap> for Method {
    fn from(value: MethodWrap) -> Self {
        value
            .0
            .expect("Unexpected error: no value stored in this MethodWrap object")
    }
}
impl From<Option<Method>> for MethodWrap {
    fn from(value: Option<Method>) -> Self {
        Self(value)
    }
}
impl From<MethodWrap> for Option<Method> {
    fn from(value: MethodWrap) -> Self {
        value.0
    }
}
```

> [!TIP]
> Keep the encoding stable. Your `as_value` output becomes the output format for that field.

*With this arsenal, your entities hit every target, every time.*
