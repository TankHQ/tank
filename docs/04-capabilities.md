# Capabilities
###### *Field Manual Section 4* - Operational Limits

Not all tanks are built for every terrain. While Tank provides a unified API, the underlying engines (Postgres, Redis, Mongo, etc.) have vastly different mechanical limits. This matrix details which operations are natively supported, emulated, or impossible on each driver.

## Feature Matrix

| Feature          | Postgres | SQLite    | MySQL/MariaDB   | DuckDB | MongoDB  | ScyllaDB/Cassandra  | Valkey/Redis  |
| ---------------- | :------: | :-------: | :-------------: | :----: | :------: | :-----------------: | :-----------: |
| **Transaction**  | ✅       | ✅        | ✅              | ✅     | ✅       | ⚠️                  | ⚠️            |
| **Join**         | ✅       | ✅        | ✅              | ✅     | ❌       | ❌                  | ❌            |
| **Bulk Append**  | ✅       | ❌        | ❌              | ✅     | ❌       | ❌                  | ❌            |
| **Filtering**    | ✅       | ✅        | ✅              | ✅     | ✅       | ⚠️                  | ❌            |
| **Aggregations** | ✅       | ✅        | ✅              | ✅     | ✅       | ❌                  | ❌            |

> [!WARNING]
> - **ScyllaDB/Cassandra** manages transactions using the [batch](https://docs.scylladb.com/manual/stable/cql/dml/batch.html) feature. It can only execute modify statements and it is atomic only within a partition. Moreover the batch accumulates the commands and sends them on commit.

*Study the specs. Pick the right armor. Dominate the field.*
