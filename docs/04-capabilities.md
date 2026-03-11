# Capabilities
###### *Field Manual Section 4* - Operational Limits

Not all tanks are built for every terrain. While Tank provides a unified API, the underlying engines (Postgres, Redis, Mongo, etc.) have vastly different mechanical limits. This matrix details which operations are natively supported, emulated, or impossible on each driver.

## Feature Matrix

| Feature              | Postgres   | SQLite    | MySQL/MariaDB   | DuckDB | MongoDB  | ScyllaDB/Cassandra  | Valkey/Redis  |
| -------------------- | :--------: | :-------: | :-------------: | :----: | :------: | :-----------------: | :-----------: |
| **Transactions**     | ✅         | ✅        | ✅              | ✅     | ✅       | ❌                  | ❌            |
| **Joins**            | ✅         | ✅        | ✅              | ✅     | ❌       | ❌                  | ❌            |
| **Bulk Insert**      | ✅         | ✅        | ✅              | ✅     | ✅       | ✅                  | ✅            |
| **Filters (WHERE)**  | ✅         | ✅        | ✅              | ✅     | ✅       | ⚠️                  | ❌            |
| **Aggregations**     | ✅         | ✅        | ✅              | ✅     | ✅       | ❌                  | ❌            |

## Semantic Differences

### Transactions
- **Interactive**: Standard SQL drivers and MongoDB allow you to read, perform logic, and write within the same transaction context.
- **Batch / Pipeline**: ScyllaDB and Valkey operate on a queue-and-commit model. You queue up multiple write operations, and they are executed atomically when you commit. You cannot read data from the transaction before it is committed.

### Filters
- **Standard**: SQL drivers and MongoDB support complex `WHERE` clauses with nested conditions, ranges, and logical operators.
- **Key-Value / Wide-Column**: ScyllaDB and Valkey are optimized for primary key lookups. ScyllaDB supports some filtering on clustering keys and secondary indexes but with performance caveats. Valkey only supports direct key lookups.

### Returning Data
- **RETURNING**: Postgres, SQLite, DuckDB, and MongoDB can return the fully generated row (including auto-increment IDs and default values) immediately after insertion.
- **LastInsertId**: MySQL provides the ID of the last inserted row, but not the full row content. Tank handles this transparently where possible.
- **None**: ScyllaDB and Valkey do not return the inserted data. You must know the ID beforehand or query it separately (which may not be consistent in eventually consistent systems).

*Study the specs. Pick the right armor. Dominate the field.*
