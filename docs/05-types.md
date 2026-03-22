# Types
###### *Field Manual Section 5* - Payload Specs

Tank brings a full type arsenal to the field. The `Entity` derive macro identifies the type you're using by inspecting its final path segment (the "trailer"). For example, `std::collections::VecDeque`, `collections::VecDeque`, or simply `VecDeque` all resolve to the same list type.

Tank maps ordinary Rust types (numbers, strings, times, collections) to the closest column types each driver supports, falling back to generic representations when appropriate. Below is the standard mapping of Rust types to each driver's column type. `:x:` indicates no native support at this time. Collection types may be emulated in some drivers using generic JSON or text representations.

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
> When a type falls back to a generic representation (e.g. `TEXT` or `JSON`), Tank encodes it predictably so equality and ordering comparisons (where meaningful) behave as expected. Advanced indexing or operator support may vary by driver.
>
> The special `isize`/`usize` types map to the native pointer-width integer (64‑bit on 64‑bit targets, 32‑bit on 32‑bit targets). For cross‑database portability prefer explicit `i64`/`u64` unless you truly need platform width.

## Wrapper Types
Built‑in wrappers you can use directly in entities. SQL type is inferred from the inner type.

Supported wrappers:
- `tank::Passive<T>`: Omit on update or allow default generation on insert.
- `Option<T>`: Nullable column.
- `Box<T>`
- `Cell<T>`
- `RefCell<T>`
- `RwLock<T>`
- `Arc<T>`
- `Rc<T>`

## Custom Types
When standard types miss the mark, deploy custom payloads: an enum that must round‑trip cleanly across drivers, or a small struct you want to pack into a single column. In Tank, you do that by implementing [`tank::AsValue`](https://docs.rs/tank/latest/tank/trait.AsValue.html).

`AsValue` is your conversion contract: it turns your Rust type into a [`tank::Value`](https://docs.rs/tank/latest/tank/enum.Value.html) for binding/inserts/updates, and turns a `Value` back into your type when decoding rows. Once implemented, you can use the type directly as an `Entity` field (including `Option<T>`).

### Example: Custom Struct
If you want a small struct to live in a single column, encode it into a stable representation (a compact string is often the most portable). Here’s a `host:port` example:

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
	fn try_from_value(value: Value) -> Result<Self> {
		if let Value::Varchar(Some(v), ..) = value.try_as(&Value::Varchar(None))? {
			let (host, port) = v
				.split_once(':')
				.ok_or_else(|| Error::msg(format!("Invalid HostPort `{v}`")))?;

			return Ok(HostPort {
				host: host.to_string(),
				port: port
					.parse::<u16>()
					.map_err(|_| Error::msg(format!("Invalid port in HostPort `{v}`")))?,
			});
		}
		Err(Error::msg("Unexpected value for HostPort"))
	}
}
```

### Example: Conversion Type
When the custom type lives outside your crate you can use intermediary wrapper type you can control and can implement `AsValue`.

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
    pub method: Method, // Method is a third party type
    pub beign_timestamp_ms: i64,
    pub end_timestamp_ms: Option<i64>,
}
#[derive(Clone, PartialEq, Eq, tank::Entity, Debug)]
pub struct RequestLimit {
    #[tank(primary_key)]
    pub id: i64,
    pub target_pattern: String,
    pub requests: i32,
    #[tank(conversion_type = MethodWrap)]
    pub method: Option<Method>, // Method is a third party type
    pub time_interval_ms: Option<i32>,
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
        let context = || {
            format!(
                "Could not conver {value:?} into {}",
                any::type_name::<Method>()
            )
        };
        match &value {
            tank::Value::Varchar(Some(value), ..) => {
                Ok(Method::from_str(&value).with_context(context)?.into())
            }
            _ => Err(tank::Error::msg(context())),
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
> Keep the encoding stable and non-lossy. Your `as_value` output becomes the output format for that field.

*With this arsenal, your entities hit every target, every time.*
