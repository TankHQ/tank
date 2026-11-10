# Capabilities
###### *Field Manual Section 4* - Operational Limits

Not all tanks are built for every terrain. While Tank provides a unified API, the underlying engines (Postgres, Redis, Mongo, etc.) have vastly different mechanical limits. This matrix details which operations are natively supported, emulated, or impossible on each driver.

## Feature Matrix

| Feature          | Postgres | SQLite    | MySQL/MariaDB   | DuckDB | ClickHouse | chDB | MongoDB  | ScyllaDB/Cassandra  | Valkey/Redis  |
| ---------------- | :------: | :-------: | :-------------: | :----: | :--------: | :--: | :------: | :-----------------: | :-----------: |
| **Transaction**  | ✅       | ✅        | ✅              | ✅     | ❌         | ❌   | ✅       | ⚠️                  | ⚠️            |
| **Join**         | ✅       | ✅        | ✅              | ✅     | ✅         | ✅   | ❌       | ❌                  | ❌            |
| **Bulk Append**  | ✅       | ❌        | ❌              | ✅     | ❌         | ❌   | ❌       | ❌                  | ❌            |
| **Filtering**    | ✅       | ✅        | ✅              | ✅     | ✅         | ✅   | ✅       | ⚠️                  | ⚠️            |
| **Aggregations** | ✅       | ✅        | ✅              | ✅     | ✅         | ✅   | ✅       | ⚠️                  | ❌            |

✅ supported, ⚠️ limited support, ❌ unavailable through Tank. **Bulk Append** means a specialized ingestion path; `insert_many` still works without one.

> [!WARNING]
> - **ScyllaDB/Cassandra** uses logged [batches](https://docs.scylladb.com/manual/stable/cql/dml/batch.html), accumulating mutations until commit. Logged batches provide eventual all-or-none completion, but isolation only within a partition. Transactional reads are unavailable. CQL aggregate functions such as `MAX` are supported. SQL-style grouping and `HAVING` are not generally portable.
> - **Valkey/Redis** queues commands until commit using a non-atomic pipeline, not `MULTI`/`EXEC`. Transactional reads are unavailable, and rollback cannot undo submitted commands. Only primary-key equality predicates are supported for filtering.
> - **MongoDB** transactions require a replica set or sharded deployment.
> - **ClickHouse** and **chDB** do not support transactions. `begin()` returns an error. Issue statements directly against the connection.

*Study the specs. Pick the right armor. Dominate the field.*
