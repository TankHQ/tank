# Connection
###### *Field Manual Section 3* - Supply Lines

Before you can unleash Tank's firepower, secure your supply lines. Open a **Connection** to your database and, where supported, group operations inside a **Transaction**.

## Operations Briefing
- [`prepare("SELECT...")`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.prepare):
  Prepare a raw SQL string as a reusable [`Query<Driver>`](https://docs.rs/tank/latest/tank/enum.Query.html) without firing it, if the driver supports SQL preparation. Use when the same statement will be dispatched multiple times.

- [`run(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#tymethod.run):
  Streams [`QueryResult`](https://docs.rs/tank/latest/tank/enum.QueryResult.html) items (`Row` or `Affected`). Useful for multi-statement batches (if supported by the database driver).

- [`fetch(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.fetch):
  Streams only [`Row`](https://docs.rs/tank/latest/tank/trait.Row.html) values, discarding [`QueryResult::Affected`](https://docs.rs/tank/latest/tank/enum.QueryResult.html).

- [`execute(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.execute):
  Aggregates all `QueryResult::Affected` counts into one [`RowsAffected`](https://docs.rs/tank/latest/tank/struct.RowsAffected.html). Rows are ignored.

- [`append(entities)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.append):
  Bulk insert entities, using a driver fast-path when available.

- [`begin()`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.begin):
  Borrow the connection and start a transaction. Issue supported operations against the transactional executor, then await `commit()` or `rollback()`. Rollback on drop is driver-specific, so do not rely on it.

## Connection Lifecycle
1. **Establish**: Connect to the database with a URL.
    * **Preferred**: [`driver.connect_pool("dbms://...", PoolConfig::new()).await?`](https://docs.rs/tank/latest/tank/trait.Driver.html#method.connect_pool) creates a managed pool. Call `.get().await?` to borrow a [`PooledConnection`](https://docs.rs/tank/latest/tank/struct.PooledConnection.html) from it.
    * **Direct**: [`PostgresConnection::connect(&driver, "postgres://...".into()).await?`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.connect) opens a single unmanaged connection using the driver-specific connection type.

2. **Deploy**: Use the connection for queries, inserts, updates, and deletes.
3. **Lock (optional)**: Start a transaction with [`connection.begin().await?`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.begin). This borrows the connection; all operations route through the transactional executor until `commit()` or `rollback()`.
4. **Terminate**: Direct connections close when dropped. Pooled connections return to their pool instead. [`disconnect().await?`](https://docs.rs/tank/latest/tank/trait.Connection.html#method.disconnect) requests an explicit shutdown when supported, but does not force a pooled connection to close.

## Connect
Every database connection abstraction implements the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) trait. This is your communication link to the database server. Call [`driver.connect_pool("dbms://...", PoolConfig::new())`](https://docs.rs/tank/latest/tank/trait.Driver.html#method.connect_pool) with a URL to create a connection pool, then call `.get().await?` to obtain a [`PooledConnection`](https://docs.rs/tank/latest/tank/struct.PooledConnection.html) from it. Every driver is its own crate. Load only what you need for the operation. Check the [drivers](01-introduction.md#drivers) to see the available connections.

Once the line is open, the connection exposes both the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) and [`Executor`](https://docs.rs/tank/latest/tank/trait.Executor.html) interfaces. Supported operations depend on the driver: MongoDB and Valkey use native query representations rather than raw SQL or SQL preparation. Check the [capability matrix](04-capabilities.md) before deploying.

The helpers below borrow a caller-owned driver and return opaque pools without boxing. Create the driver in the caller (e.g. `PostgresDriver::new()`), pass it by reference, and keep it alive while using the returned pool. See [Dynamic Pool](#dynamic-pool) when you need type erasure or a shared handle.

### Postgres
Postgres is your heavy artillery: powerful, networked, built for sustained campaigns with multiple units coordinating strikes.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_postgres::PostgresDriver;

async fn establish_postgres_connection(driver: &PostgresDriver) -> Result<impl ConnectionPool<PostgresDriver>> {
    let pool = driver
        .connect_pool(
            "postgres://tank-user:armored@127.0.0.1:5432/military?sslmode=require&sslrootcert=ROOT_PATH&sslcert=CERT_PATH&sslkey=KEY_PATH",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `postgres://user:pass@host:port/database`

Parameters:
- `sslmode`: TLS negotiation mode. When omitted, the driver consults `PGSSLMODE`, falling back to `disable` if that variable is also unset. To require TLS, set `sslmode=require` explicitly in the URL rather than relying on the environment variable. The supported `tokio_postgres` modes are:
    - `disable`
    - `prefer`
    - `require`
- `sslrootcert`: CA certificate path (or environment variable `PGSSLROOTCERT`).
- `sslcert`: Client certificate path (or environment variable `PGSSLCERT`).
- `sslkey`: Client private key path (or environment variable `PGSSLKEY`).

### SQLite
SQLite is the lone wolf operative, deep behind enemy lines: lightweight, reliable, zero configuration. Deploy anywhere, anytime.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_sqlite::SQLiteDriver;

async fn establish_sqlite_connection(driver: &SQLiteDriver) -> Result<impl ConnectionPool<SQLiteDriver>> {
    let pool = driver
        .connect_pool(
            "sqlite://database.sqlite?mode=rwc",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- File: `sqlite://path/to/database.sqlite?mode=rwc`
- Memory: `sqlite://:memory:` or `sqlite://database?mode=memory`

Modes:
- `mode=ro`: read-only access (fails if the file doesn’t exist).
- `mode=rw`: read-write access (fails if the file doesn’t exist).
- `mode=rwc`: read-write access (creates the file if it doesn’t exist).
- `mode=memory`: in-memory access (temporary database that lives only for the duration of the connection).

Additional URL parameters are passed directly to the SQLite API. See the full list of supported options on the [SQLite website](https://sqlite.org/uri.html#recognized_query_parameters).

### MySQL/MariaDB
MySQL is the battle-hardened workhorse of the digital front: widely deployed, solid transactional engine, broad tooling ecosystem.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_mysql::MySQLDriver;

async fn establish_mysql_connection(driver: &MySQLDriver) -> Result<impl ConnectionPool<MySQLDriver>> {
    let pool = driver
        .connect_pool(
            "mysql://tank-mysql-user@localhost:3306/operations_db?require_ssl=true&ssl_ca=/home/user/Git/tank/tank-mysql/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-mysql/tests/assets/client-cert.pem&ssl_key=/home/user/Git/tank/tank-mysql/tests/assets/client-key.pem",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `mysql://user:password@host:port/database`

Parameters:
- `require_ssl (bool)`: Require secure connection, defaults to false.
- `ssl_ca`: CA certificate path (falls back to environment variable `MYSQL_SSL_CA`).
- `ssl_cert`: Client certificate path (falls back to environment variable `MYSQL_SSL_CERT`).
- `ssl_key`: Client private key path (falls back to environment variable `MYSQL_SSL_KEY`).

Additional URL parameters are passed directly to the mysql_async API. See the full list of supported options from options structure [Opts](https://docs.rs/mysql_async/latest/mysql_async/struct.Opts.html).

> [!NOTE]
> **MariaDB support**: use `MariaDBDriver::mariadb()` with a `mysql://` URL to enable MariaDB mode. When active, Tank uses MariaDB native column types where they differ from MySQL, most notably `UUID` instead of `CHAR(36)` (MariaDB 10.7+).
>
> Use the `column_type` attribute for other native DDL (e.g. `#[tank(column_type = (mariadb = "INET6", mysql = "VARCHAR(39)"))]` on a `String` field). The `mariadb` key applies only in MariaDB mode; the `mysql` key applies to both modes.

### DuckDB
DuckDB is your embedded artillery piece: fast, local, and always ready. Perfect for rapid deployment scenarios and testing under fire.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_duckdb::DuckDBDriver;

async fn establish_duckdb_connection(driver: &DuckDBDriver) -> Result<impl ConnectionPool<DuckDBDriver>> {
    let pool = driver
        .connect_pool(
            "duckdb://database.duckdb?mode=rw",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- File: `duckdb://path/to/database.duckdb?mode=rw`
- Memory: `duckdb://:memory:` or `duckdb://database?mode=memory`

Modes:
- `mode=ro`: read-only access (fails if the file doesn’t exist)
- `mode=rw`: read-write access (creates the file if it doesn’t exist)
- `mode=rwc`: alias for `rw`
- `mode=memory`: in-memory access (temporary database that lives only for the duration of the connection)

The `mode` parameter provides a common syntax for specifying connection access, similar to SQLite. The values map respectively to `access_mode=READ_ONLY`, `access_mode=READ_WRITE`, `access_mode=READ_WRITE` and the special `duckdb://:memory:` path. Additional URL parameters are passed directly to the DuckDB C API. See the full list of supported options on the [DuckDB website](https://duckdb.org/docs/stable/configuration/overview#global-configuration-options).

### ClickHouse
ClickHouse is your long-range artillery battery: columnar, massively parallel, built to flatten mountains of data into a single decisive figure.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_clickhouse::ClickHouseDriver;

async fn establish_clickhouse_connection(driver: &ClickHouseDriver) -> Result<impl ConnectionPool<ClickHouseDriver>> {
    let pool = driver
        .connect_pool(
            "clickhouse://default@127.0.0.1:9000/default",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `clickhouse://user:password@host:port/database`

The driver uses plaintext native TCP, not HTTP, and exposes no TLS configuration. Specify a hostname and an explicit native port (normally `9000`). Omitted credentials use user `default` and an empty password. An omitted database is left unspecified in the client options, so the server chooses its default.

The only supported query parameter is `tcp_nodelay`: it defaults to `true` and accepts only `true` or `false`. Unknown parameters and invalid boolean values return errors. Supply credentials and the database in the URL authority and path, not query parameters.

> [!NOTE]
> **No transactions or multi-statement execution**: `begin()` returns an error and `commit`/`rollback` are unsupported; issue one statement at a time directly against the connection.
>
> On connect, Tank applies `allow_experimental_lightweight_delete=1`, `join_use_nulls=1`, and `final=1`. If any session setting fails, the connection attempt fails. See [ClickHouse/chDB capabilities](04-capabilities.md#clickhouse-and-chdb) for engine and deduplication semantics.

### chDB
chDB is mobile artillery: long-range firepower embedded in your own line, no emplacement, no supply line.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_chdb::ChDBDriver;

async fn establish_chdb_connection(driver: &ChDBDriver) -> Result<impl ConnectionPool<ChDBDriver>> {
    let pool = driver
        .connect_pool("chdb://", PoolConfig::new())
        .await?;
    Ok(pool)
}
```

**URL Format**:
- Memory: `chdb://` opens a fresh in-memory database
- File: `chdb://?path=/path/to/database`

The first nonempty, case-insensitive `path` query parameter takes precedence. Otherwise, the driver uses the URL path with leading slashes stripped, so `chdb:///relative/database` selects a relative directory. Use `?path=` for absolute paths. The URL authority and other query parameters are ignored. Without a path, the database is in memory and lasts for the connection's lifetime.

> [!NOTE]
> **No transactions or multi-statement execution**: like its server-backed sibling, chDB does not support `begin()`, `commit` or `rollback`; issue one statement at a time.
>
> On connect, Tank applies `allow_experimental_lightweight_delete=1`, `join_use_nulls=1`, `final=1`, and `output_format_json_quote_decimals=1`. If any session setting fails, the connection attempt fails.
>
> **Blocking native work**: the async interface offloads connection opening and query initialization to blocking workers, but subsequent chunk fetches can block while polling the stream, and cancellation/cleanup can block when it is dropped.

See [ClickHouse/chDB capabilities](04-capabilities.md#clickhouse-and-chdb), [type mappings](05-types.md#clickhouse-and-chdb), and [raw SQL preparation](09-raw-sql.md#clickhouse-and-chdb) for shared behavior and limitations.

### MongoDB
MongoDB is your guerrilla special forces unit operating in the "fog of war", gathering intel in whatever format it arrives.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_mongodb::MongoDBDriver;

async fn establish_mongodb_connection(driver: &MongoDBDriver) -> Result<impl ConnectionPool<MongoDBDriver>> {
    let pool = driver
        .connect_pool(
            "mongodb://tank-user:armored@127.0.0.1:27017/military?directConnection=true&authSource=admin&tls=true&tlsCAFile=/home/user/Git/tank/tank-mongodb/tests/assets/ca.pem&tlsCertificateKeyFile=/home/user/Git/tank/tank-mongodb/tests/assets/client.pem",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `mongodb://username:password@host:port/database`

The database name is required in the URL path. Tank returns an error if it is missing.

### Valkey/Redis
Valkey is your in-memory supply depot for caches, sessions, queues, rate limits, and hot-path counters. This driver speaks both Valkey and Redis.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_valkey::ValkeyDriver;

async fn establish_valkey_connection(driver: &ValkeyDriver) -> Result<impl ConnectionPool<ValkeyDriver>> {
    let pool = driver
        .connect_pool(
            "valkeys://valkey-commander:supreme@127.0.0.1:32823/0?sslrootcert=/home/user/Git/tank/tank-valkey/tests/assets/ca.pem&sslcert=/home/user/Git/tank/tank-valkey/tests/assets/client-cert.pem&sslkey=/home/user/Git/tank/tank-valkey/tests/assets/client-key.pem",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `valkeys://username:password@host:port/n?...tls_params`
- `valkey://username:password@host:port/n`
- `rediss://username:password@host:port/n?...tls_params`
- `redis://username:password@host:port/n`

Use `valkeys://` or `rediss://` for TLS. The `sslmode` query parameter does not enable TLS for this driver.

Parameters:
- `sslrootcert`: CA certificate path.
- `sslcert`: Client certificate path.
- `sslkey`: Client private key path.

### ScyllaDB/Cassandra
ScyllaDB is the rapid-response strike force: distributed, built to swarm data with relentless, low-latency fire.

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_scylladb::ScyllaDBDriver;

async fn establish_scylla_connection(driver: &ScyllaDBDriver) -> Result<impl ConnectionPool<ScyllaDBDriver>> {
    let pool = driver
        .connect_pool(
            "scylladb://localhost:9142/scylla_keyspace?ssl_ca=/home/user/Git/tank/tank-scylladb/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-scylladb/tests/assets/client-cert.pem&ssl_key=/home/user/Git/tank/tank-scylladb/tests/assets/client-key.pem",
            PoolConfig::new(),
        )
        .await?;
    Ok(pool)
}
```

**URL Format**:
- `scylladb://host:9042/keyspace`
- `cassandra://host:9042/keyspace`

Parameters:
- `ssl_ca`: Path to the CA certificate file.
- `ssl_cert`: Path to the client certificate file (PEM format).
- `ssl_key`: Path to the client private key file (PEM format).
- `local_ip_address`: Binds the connection to a specific local IP.
- `connection_timeout (f64)`: Connection-establishment timeout in seconds, the default is 5.
- `hostname_resolution_timeout (f64)`: DNS resolution timeout in seconds.
- `tcp_nodelay (bool)`: Set the nodelay TCP flag, true by default.
- `tcp_keepalive_interval (f64)`: Interval between keepalive TCP messages in seconds, by default no keepalive messages are sent.
- `keepalive_interval (f64)`: Interval in seconds between keepalive CQL messages, the default is 30.
- `disallow_shard_aware_port (bool)`: Prevents the driver from connecting to the shard-aware port, even if the node supports it (ScyllaDB only).
- `compression`: Data compression algorithm, no compression by default:
    - `lz4`
    - `snappy`
- `pool_size_per_host`: Number of connections maintained per host.
- `pool_size_per_shard`: Number of connections maintained per shard, the default is 1. If both pool-size options are supplied, the last one in the URL wins.
- `write_coalescing_delay (positive int or "SmallNondeterministic")`: Delay before flushing data to the socket. Integer values are milliseconds.
- `use_keyspace`: Sets the active keyspace.
- `keyspaces_to_fetch`: Specific keyspaces to fetch metadata for, by default all keyspaces will be fetched.
- `fetch_schema_metadata (bool)`: True by default.
- `cluster_metadata_refresh_interval (f64)`: Interval in seconds at which the driver refreshes the cluster metadata (topology and schema), the default is 60.
- `metadata_request_serverside_timeout (f64)`: Server-side timeout in seconds for metadata queries, the current Scylla client default is 30.
- `schema_agreement_interval (f64)`: Polling frequency in seconds for verifying cluster-wide schema consistency, the default is 0.2 (200ms).
- `schema_agreement_timeout (f64)`: Timeout in seconds for waiting for schema agreement.
- `auto_await_schema_agreement (bool)`: Automatically wait for schema agreement after executing schema-altering statements, true by default.
- `refresh_metadata_on_auto_schema_agreement (bool)`: Refreshes metadata automatically when schema agreement is reached.
- `tracing_info_fetch_attempts`: Number of retry attempts to fetch tracing info.
- `tracing_info_fetch_interval`: Wait time in seconds between tracing fetch attempts, the default is 0.003 (3ms).
- `tracing_info_fetch_consistency`: Consistency level for tracing info (mapped to Scylla's internal u16 consistency levels).

The parameters are used to create an object of type [`SessionBuilder`](https://docs.rs/scylla/latest/scylla/client/session_builder/type.SessionBuilder.html). Please check the Scylla documentation for more detailed information.

## Dynamic Pool

`driver.connect_pool(...)` returns an opaque `impl ConnectionPool<Driver>` type. The compiler knows its concrete type, but you cannot name that return type directly in a struct field.

Use [`into_box()`](https://docs.rs/tank/latest/tank/trait.ConnectionPool.html#tymethod.into_box) or [`into_arc()`](https://docs.rs/tank/latest/tank/trait.ConnectionPool.html#tymethod.into_arc) to type-erase the pool into a `Box<dyn ConnectionPool<D>>` or `Arc<dyn ConnectionPool<D>>` respectively. Both can be stored in struct fields; use `Arc` for a clonable shared handle:

```rust
use tank::{ConnectionPool, Driver, PoolConfig, Result};
use tank_postgres::PostgresDriver;

type DBType = PostgresDriver;
struct AppState {
    db_connection: Box<dyn ConnectionPool<DBType>>,
}

impl AppState {
    async fn new(db_url: &'static str) -> Result<Self> {
        let pool = DBType::new()
            .connect_pool(db_url, PoolConfig::new())
            .await?
            .into_box();
        Ok(Self { db_connection: pool })
    }
}
```

The boxed pool is used identically to the concrete pool. Call `.get().await?` to borrow a connection, and all query operations work as usual:

```rust
async fn run(state: &AppState) -> Result<()> {
    let mut connection = state.db_connection.get().await?;
    Tank::create_table(&mut connection, true, true).await?;
    Ok(())
}
```

*Lock, commit, advance. Dismissed.*
