# Connection
###### *Field Manual Section 3* - Supply Lines

Welcome to the armored convoy, commander. Before you can unleash Tank's firepower, you have to secure your supply lines. Open a **Connection** to your database, and when the mission escalates, lock operations inside a **Transaction**. No connection, no combat. It's that simple.

## Connect
Every database connection abstraction implements the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) trait. This is your communication link to the database server. Call [`driver.connect("dbms://...")`](https://docs.rs/tank/latest/tank/trait.Driver.html#method.connect) with a URL to let Tank establish the line. Every driver is its own crate. Load only what you need for the operation. Check the [drivers](1-introduction.md#drivers) to see the available connections.

Once the line is open, the connection exposes both the [`Connection`](https://docs.rs/tank/latest/tank/trait.Connection.html) and [`Executor`](https://docs.rs/tank/latest/tank/trait.Executor.html) interfaces, enabling you to prepare statements, run multiple queries, execute commands, fetch rows and orchestrate transactions.

### DuckDB
DuckDB is your embedded artillery piece: fast, local, and always ready. Perfect for rapid deployment scenarios and testing under fire.

```rust
use tank::Driver;
use tank_duckdb::{DuckDBConnection, DuckDBDriver};

async fn establish_duckdb_connection() -> Result<DuckDBConnection> {
    let driver = DuckDBDriver::new();
    let connection = driver
        .connect("duckdb://../target/debug/combat.duckdb?mode=rw".into())
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

### SQLite
SQLite is the lone wolf operative, deep behind enemy lines: lightweight, reliable, zero configuration. Deploy anywhere, anytime.

```rust
use tank::Driver;
use tank_sqlite::{SQLiteConnection, SQLiteDriver};

async fn establish_sqlite_connection() -> Result<SQLiteConnection> {
    let driver = SQLiteDriver::new();
    let connection = driver
        .connect("sqlite://../target/debug/operations.sqlite?mode=rwc".into())
        .await?;
    Ok(connection)
}
```

**URL Format**:
- File: `sqlite://path/to/database.sqlite?mode=rwc`
- Memory: `sqlite://:memory:` or `sqlite://database?mode=memory`

Modes:
- `mode=ro`: read-only access (fails if the file doesn’t exist)
- `mode=rw`: read-write access (fails if the file doesn’t exist)
- `mode=rwc`: read-write access (creates the file if it doesn’t exist)
- `mode=memory`: in-memory access (temporary database that lives only for the duration of the connection)

Additional URL parameters are passed directly to the SQLite API. See the full list of supported options on the [SQLite website](https://sqlite.org/uri.html#recognized_query_parameters).

### Postgres
Postgres is your heavy artillery: powerful, networked, built for sustained campaigns with multiple units coordinating strikes.

```rust
use tank::Driver;
use tank_postgres::{PostgresConnection, PostgresDriver};

async fn establish_postgres_connection() -> Result<PostgresConnection> {
    let driver = PostgresDriver::new();
    let connection = driver
		.connect("postgres://tank-user:armored@127.0.0.1:32790/military?sslmode=require&sslrootcert=ROOT_PATH&sslcert=CERT_PATH&sslkey=KEY_PATH".into())
    	.await?;
    Ok(connection)
}
```

**URL Format**:
- `postgres://user:pass@host:5432/database`

Parameters:
- `sslmode`: How a secure SSL TCP/IP connection will be negotiated with the server. Falls back to the environment variable `PGSSLMODE`, otherwise `disable`. This parameter is passed to `tokio_postgres`, for this reason only the following alternatives are supported (even tough Postgres supports more modes):
    - `disable`
    - `prefer`
    - `require`
- `sslrootcert`: CA certificate path (falls back to environment variable `PGSSLROOTCERT` or `~/.postgresql/root.crt`).
- `sslcert`: Client certificate path (falls back to environment variable `PGSSLCERT` or `~/.postgresql/postgresql.crt`).
- `sslkey`: Client private key path (falls back to environment variable `PGSSLKEY` or `~/.postgresql/postgresql.key`).

### MySQL / MariaDB
MySQL is the battle-hardened workhorse of the digital front: widely deployed, solid transactional engine, broad tooling ecosystem.

```rust
use tank::Driver;
use tank_mysql::{MySQLConnection, MySQLDriver};

async fn establish_mysql_connection() -> Result<MySQLConnection> {
  let driver = MySQLDriver::new();
  let connection = driver
    .connect("mysql://tank-mysql-user@localhost:33231/operations_db?require_ssl=true&ssl_ca=/home/user/Git/tank/tank-mysql/tests/assets/ca.pem&ssl_cert=/home/user/Git/tank/tank-mysql/tests/assets/client.p12&ssl_pass=my%26pass%3Fis%3DP%40%24%24".into())
    .await?;
  Ok(connection)
}
```

**URL Format**:
- `mysql://user@host:port/database?require_ssl=true&ssl_ca=CA_PATH&ssl_cert=CERT_PATH&ssl_pass=CERT_PASS`

Parameters:
- `require_ssl (bool)`: Require secure connection, defaults to false.
- `ssl_ca`: CA certificate path (falls back to environment variable `MYSQL_SSL_CA`).
- `ssl_cert`: Client certificate path (falls back to environment variable `MYSQL_SSL_CERT`).

Additional URL parameters are passed directly to the mysql_async API. See the full list of supported options from options structure [Opts](https://docs.rs/mysql_async/latest/mysql_async/struct.Opts.html).

### ScyllaDB / Cassandra
ScyllaDB is the rapid‑response strike force: distributed, built to swarm data with relentless, low‑latency fire.

```rust
use tank::Driver;
use tank_scylladb::{ScyllaConnection, ScyllaDriver};

async fn establish_scylla_connection() -> Result<ScyllaConnection> {
  let driver = ScyllaDriver::new();
  let connection = driver
    .connect("scylladb://127.0.0.1:9042/keyspace_name".into())
    .await?;
  Ok(connection)
}
```

**URL Format**:
- `scylla://host1,host2:9042/keyspace?consistency=quorum&compression=Lz4`

Parameters:
- `consistency`: Query consistency level (examples: `one`, `quorum`, `all`).
- `timeout_ms`: Request timeout in milliseconds.

## Operations Briefing
- [`prepare("SELECT * FROM ...*")`](https://docs.rs/tank/latest/tank/trait.Executor.html#tymethod.prepare):
  Compiles a raw SQL string into a reusable [`Query<Driver>`](https://docs.rs/tank/latest/tank/enum.Query.html) object without firing it. Use when the same statement will be dispatched multiple times.

- [`run(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#tymethod.run):
  Streams [`QueryResult`](https://docs.rs/tank/latest/tank/enum.QueryResult.html) items (`Row` or `Affected`). Useful for multi‑statement batches (if supported by the database driver).

- [`fetch(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.fetch):
  Streams only rows (`QueryResult::Row`), discarding `Affected`.

- [`execute(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.execute):
  Aggregates all `Affected` counts into one `RowsAffected`. Rows are ignored.

- [`append(query)`](https://docs.rs/tank/latest/tank/trait.Executor.html#method.append):
  Bulk insert entities, using driver fast‑path when available.

- [`begin()`](https://docs.rs/tank/latest/tank/trait.Connection.html#tymethod.begin):
  Borrow the connection and start a transaction. Issue any of the above operations against the transactional executor, then `commit` or `rollback`. Uncommitted drop triggers a rollback and gives back the connection.

## Connection Lifecycle
1. **Establish**: Call `driver.connect("dbms://...").await?` with your database URL.
2. **Deploy**: Use the connection for queries, inserts, updates, and deletes.
3. **Lock (optional)**: Start a transaction with `connection.begin().await?`, this borrows the connection. All operations route through the transactional executor until `commit()` or `rollback()`.
4. **Maintain**: Current drivers expose a single underlying session (DuckDB shares process instance; Postgres spawns one async connection; SQLite opens one handle). External pooling is not bundled.
5. **Terminate**: Connections close automatically when dropped. Disconnection is ensured after a call to `disconnect().await`.

*Lock, commit, advance. Dismissed.*
