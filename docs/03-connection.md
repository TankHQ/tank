# Connection
###### *Field Manual Section 3* - Supply Lines

Welcome to the armored convoy, commander. Before you can unleash Tank's firepower, you have to secure your supply lines. Open a **Connection** to your database, and when the mission escalates, lock operations inside a **Transaction**. No connection, no combat. It's that simple.

## Operations Briefing
- [`prepare("SELECT...")`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.prepare):
  Compile a raw SQL string into a reusable [`Query<Driver>`](https://docs.rs/tank/latest/tank/enum.Query.html) object without firing it. Use when the same statement will be dispatched multiple times.

- [`run(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#tymethod.run):
  Streams [`QueryResult`](https://docs.rs/tank/latest/tank/enum.QueryResult.html) items (`Row` or `Affected`). Useful for multi-statement batches (if supported by the database driver).

- [`fetch(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.fetch):
  Streams only [`QueryResult::Row`](https://docs.rs/tank/latest/tank/struct.Row.html), discarding [`QueryResult::Affected`](https://docs.rs/tank/latest/tank/struct.RowsAffected.html).

- [`execute(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.execute):
  Aggregates all `QueryResult::Affected` counts into one [`RowsAffected`](https://docs.rs/tank/latest/tank/struct.RowsAffected.html). Rows are ignored.

- [`append(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.append):
  Bulk insert entities, using a driver fast-path when available.

- [`begin()`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.begin):
  Borrow the connection and start a transaction. Issue any of the above operations against the transactional executor, then `commit` or `rollback`. Uncommitted drop triggers a rollback and gives back the connection.

## Connection Lifecycle
1. **Establish**: Call [`driver.connect("dbms://...").await?`](https://docs.rs/tank/latest/tank/trait.Driver.html#method.connect) with your database URL.
2. **Deploy**: Use the connection for queries, inserts, updates, and deletes.
3. **Lock (optional)**: Start a transaction with [`connection.begin().await?`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.begin). This borrows the connection; all operations route through the transactional executor until `commit()` or `rollback()`.
4. **Terminate**: Connections close automatically when dropped. Call [`disconnect().await?`](https://docs.rs/tank/latest/tank/trait.Connection.html#method.disconnect) for an explicit shutdown when the driver supports it.

## Connect
Every database connection abstraction implements the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) trait. This is your communication link to the database server. Call [`driver.connect("dbms://...")`](https://docs.rs/tank/latest/tank/trait.Driver.html#method.connect) with a URL to let Tank establish the line. Every driver is its own crate. Load only what you need for the operation. Check the [drivers](01-introduction.md#drivers) to see the available connections.

Once the line is open, the connection exposes both the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) and [`Executor`](https://docs.rs/tank/latest/tank/trait.Executor.html) interfaces, enabling you to prepare statements, run multiple queries, execute commands, fetch rows and orchestrate transactions.

### Postgres
Postgres is your heavy artillery: powerful, networked, built for sustained campaigns with multiple units coordinating strikes.

```rust
use tank::Driver;
use tank_postgres::{PostgresConnection, PostgresDriver};

async fn establish_postgres_connection() -> Result<PostgresConnection> {
    let driver = PostgresDriver::new();
    let connection = driver
    .connect("postgres://tank-user:armored@127.0.0.1:5432/military?sslmode=require&sslrootcert=ROOT_PATH&sslcert=CERT_PATH&sslkey=KEY_PATH".into())
    .await?;
    Ok(connection)
}
```

**URL Format**:
- `postgres://user:pass@host:port/database`

Parameters:
- `sslmode`: How a secure SSL TCP/IP connection will be negotiated with the server. Falls back to the environment variable `PGSSLMODE`, otherwise `disable`. This parameter is passed to `tokio_postgres`, for this reason only the following alternatives are supported (even though Postgres supports more modes):
    - `disable`
    - `prefer`
    - `require`
- `sslrootcert`: CA certificate path (falls back to environment variable `PGSSLROOTCERT` or `~/.postgresql/root.crt`).
- `sslcert`: Client certificate path (falls back to environment variable `PGSSLCERT` or `~/.postgresql/postgresql.crt`).
- `sslkey`: Client private key path (falls back to environment variable `PGSSLKEY` or `~/.postgresql/postgresql.key`).

### SQLite
SQLite is the lone wolf operative, deep behind enemy lines: lightweight, reliable, zero configuration. Deploy anywhere, anytime.

```rust
use tank::Driver;
use tank_sqlite::{SQLiteConnection, SQLiteDriver};

async fn establish_sqlite_connection() -> Result<SQLiteConnection> {
    let driver = SQLiteDriver::new();
    let connection = driver
        .connect("sqlite://../target/database.sqlite?mode=rwc".into())
        .await?;
    Ok(connection)
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
use tank::Driver;
use tank_mysql::{MySQLConnection, MySQLDriver};

async fn establish_mysql_connection() -> Result<MySQLConnection> {
  let driver = MySQLDriver::new();
  let connection = driver
    .connect("mysql://tank-mysql-user@localhost:3306/operations_db?require_ssl=true&ssl_ca=/home/user/Git/tank/tank-mysql/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-mysql/tests/assets/client.p12&ssl_pass=my%26pass%3Fis%3DP%40%24%24".into())
    .await?;
  Ok(connection)
}
```

**URL Format**:
- `mysql://user:password@host:port/database`

Parameters:
- `require_ssl (bool)`: Require secure connection, defaults to false.
- `ssl_ca`: CA certificate path (falls back to environment variable `MYSQL_SSL_CA`).
- `ssl_cert`: Client certificate path (falls back to environment variable `MYSQL_SSL_CERT`).

Additional URL parameters are passed directly to the mysql_async API. See the full list of supported options from options structure [Opts](https://docs.rs/mysql_async/latest/mysql_async/struct.Opts.html).

### DuckDB
DuckDB is your embedded artillery piece: fast, local, and always ready. Perfect for rapid deployment scenarios and testing under fire.

```rust
use tank::Driver;
use tank_duckdb::{DuckDBConnection, DuckDBDriver};

async fn establish_duckdb_connection() -> Result<DuckDBConnection> {
    let driver = DuckDBDriver::new();
    let connection = driver
        .connect("duckdb://../target/debug/database.duckdb?mode=rw".into())
        .await?;
    Ok(connection)
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

### MongoDB
MongoDB is your guerrilla special forces unit operating in the "fog of war", gathering intel in whatever format it arrives.

```rust
use tank::Driver;
use tank_mongodb::{MongoDBConnection, MongoDBDriver};

async fn establish_mongodb_connection() -> Result<MongoDBConnection> {
  let driver = MongoDBDriver::new();
  let connection = driver
    .connect("mongodb://tank-user:armored@127.0.0.1:27017/military?directConnection=true&authSource=admin&tls=true&tlsCAFile=/home/user/Git/tank/tank-mongodb/tests/assets/ca.pem&tlsCertificateKeyFile=/home/user/Git/tank/tank-mongodb/tests/assets/client.pem".into())
    .await?;
  Ok(connection)
}
```

**URL Format**:
- `mongodb://username:password@host:port/database`

The database name is extracted from the URL path. If omitted, you must specify a default database in the options or rely on the driver default.

### Valkey/Redis
Valkey is your suppressive-fire support weapon: an in-memory key-value depot for caches, sessions, queues, rate limits, and hot-path counters-built for blistering throughput when the front line can’t wait. This driver speaks both Valkey and Redis.

```rust
use tank::Driver;
use tank_valkey::{ValkeyConnection, ValkeyDriver};

async fn establish_valkey_connection() -> Result<ValkeyConnection> {
    let driver = ValkeyDriver::default();
    let connection = driver
        .connect("valkeys://valkey-commander:supreme@127.0.0.1:32823/0?sslmode=require&sslrootcert=/home/user/Git/tank/tank-valkey/tests/assets/ca.pem&sslcert=/home/user/Git/tank/tank-valkey/tests/assets/client-cert.pem&sslkey=/home/user/Git/tank/tank-valkey/tests/assets/client-key.pem".into())
        .await?;
    Ok(connection)
}
```

**URL Format**:
- `valkeys://username:password@host:port/n?...tls_params`
- `valkey://username:password@host:port/n`
- `rediss://username:password@host:port/n?...tls_params`
- `redis\://username:password@host:port/n`

Parameters:
- `sslmode`: Use `require` for TLS.
- `sslrootcert`: CA certificate path.
- `sslcert`: Client certificate path.
- `sslkey`: Client private key path.

### ScyllaDB/Cassandra
ScyllaDB is the rapid-response strike force: distributed, built to swarm data with relentless, low-latency fire.

```rust
use tank::Driver;
use tank_scylladb::{ScyllaDBConnection, ScyllaDBDriver};

async fn establish_scylla_connection() -> Result<ScyllaDBConnection> {
  let driver = ScyllaDBDriver::new();
  let connection = driver
    .connect("scylladb://localhost:9142/scylla_keyspace?ssl_ca=/home/user/Git/tank/tank-scylladb/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-scylladb/tests/assets/client-cert.pem&ssl_key=/home/user/Git/tank/tank-scylladb/tests/assets/client-key.pem".into())
    .await?;
  Ok(connection)
}
```

**URL Format**:
- `scylladb://host1,host2:9042/keyspace`
- `cassandra://host1,host2:9042/keyspace`

Parameters:
- `ssl_ca`: Path to the CA certificate file.
- `ssl_cert`: Path to the client certificate file (PEM format).
- `ssl_key`: Path to the client private key file (PEM format).
- `local_ip_address`: Binds the connection to a specific local IP.
- `connection_timeout (f64)`: Request timeout in seconds, the default is 5.
- `hostname_resolution_timeout (f64)`: DNS resolution timeout in seconds.
- `tcp_nodelay (bool)`: Set the nodelay TCP flag, true by default.
- `tcp_keepalive_interval (f64)`: Interval between keepalive TCP messages in seconds, by default no keepalive messages are sent.
- `keepalive_interval (f64)`: Interval in seconds between keepalive CQL messages, the default is 30.
- `disallow_shard_aware_port (bool)`: Prevents the driver from connecting to the shard-aware port, even if the node supports it (ScyllaDB only).
- `compression`: Data compression algorithm, no compression by default:
    - `lz4`
    - `snappy`
- `pool_size_per_host`: Number of connections maintained per host, overrides `pool_size_per_shard`.
- `pool_size_per_shard`: Number of connections maintained per shard, overrides `pool_size_per_host`, the default is 1.
- `write_coalescing_delay (int or "SmallNondeterministic")`: Injects a delay before flushing data to the socket.
- `use_keyspace`: Sets the active keyspace.
- `keyspaces_to_fetch`: Specific keyspaces to fetch metadata for, by default all keyspaces will be fetched.
- `fetch_schema_metadata (bool)`: True by default.
- `cluster_metadata_refresh_interval (f64)`: Interval in seconds at which the driver refreshes the cluster metadata (topology and schema), the default is 60.
- `metadata_request_serverside_timeout (f64)`: Server-side timeout in seconds for metadata queries, the default is 2.
- `schema_agreement_interval (f64)`: Polling frequency in seconds for verifying cluster-wide schema consistency, the default is 0.2 (200ms).
- `schema_agreement_timeout (f64)`: Timeout in seconds for waiting for schema agreement.
- `auto_await_schema_agreement (bool)`: Automatically wait for schema agreement after executing schema-altering statements, true by default.
- `refresh_metadata_on_auto_schema_agreement (bool)`: Refreshes metadata automatically when schema agreement is reached.
- `tracing_info_fetch_attempts`: Number of retry attempts to fetch tracing info.
- `tracing_info_fetch_interval`: Wait time in seconds between tracing fetch attempts, the default is 0.003 (3ms).
- `tracing_info_fetch_consistency`: Consistency level for tracing info (mapped to Scylla's internal u16 consistency levels).

The parameters are used to create an object of type [`SessionBuilder`](https://docs.rs/scylla/latest/scylla/client/session_builder/type.SessionBuilder.html). Please check the Scylla documentation for more detailed information.

*Lock, commit, advance. Dismissed.*
