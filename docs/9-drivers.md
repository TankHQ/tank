# Driver Creation
###### *Field Manual Section 9* - Armored Engineering

Opening a new battlefront means forging a fresh **Driver** — the armored bridge between Tank's high‑level abstractions and a database engine's trenches (type mapping, prepared semantics, transaction doctrine). This section boots a driver crate from cold steel to live fire, then certifies it on the proving ground (`tank-tests`).

## Mission Objectives
- Stand up a new `tank-<backend>` crate
- Implement core traits: `Driver`, `Connection` + `Executor`, `Transaction` + `Executor`, `Prepared`, `SqlWriter`
- Specialize dialect printing (override only what diverges from the default functions)
- Integrate with shared test suite (`tank-tests`), gate unsupported munitions with feature flags
- Ship a lean, consistent crate aligned with existing armor plating

## Battlefield Topography
A driver is a thin composite of five parts:
| Trait         | Purpose                                                                            |
| ------------- | ---------------------------------------------------------------------------------- |
| `Driver`      | Public entry point for all the database abstractions                               |
| `Connection`  | Live session running queries and possibly starting a transaction                   |
| `Transaction` | Abstraction over transactional database capabilities, borrows mutably a connection |
| `Prepared`    | Owns a compiled statement, binds positional parameters                             |
| `SqlWriter`   | Converts Tank's operations and semantic AST fragments into backend query language  |

All other machinery (entities, expressions, joins) already speak through these interfaces.

## Forge the Crate
Create `tank-yourdb` in your favorite source repository.

`Cargo.toml` template (adjust backend dependency + features):

<<< @/../tank-yourdb/Cargo.toml

## Steps
### 1. The Driver Shell
<<< @/../tank-yourdb/src/driver.rs

### 2. Connection + Executor
Responsibilities:
- Validate / parse URL (enforce `yourdb://` prefix)
- Open / pool backend session(s)
- Implement `prepare` (compile statement) & `run` (stream `QueryResult::{Row,Affected}`)
- Optionally implement fast-path bulk `append` (DuckDB style)

Skeleton:

<<< @/../tank-yourdb/src/connection.rs

### 3. Prepared Statements
Implement parameter binding according to the backend type system. Convert each Rust value (via `AsValue`) into the native representation.

<<< @/../tank-yourdb/src/prepared.rs

### 4. SQL Writer (`SqlWriter`)
Override only differences from the generic fallback:
- Identifier quoting style
- Column type mapping
- Literal escaping quirks (BLOB, INTERVAL, UUID, arrays)
- Parameter placeholder (override `write_expression_operand_question_mark`) if not `?`
- Schema operations (skip if engine lacks schemas like SQLite)
- Upsert syntax via `write_insert_update_fragment` if divergence

Tip: Start from `tank-core`'s `GenericSqlWriter` implementation; copy then trim.

<<< @/../tank-yourdb/src/sql_writer.rs

### 5. Transactions
- Implement a `YourDBTransaction<'c>` type holding a mutable borrow of the connection.
- Provide `commit()` and `rollback()` methods, ensure resource release.
- Expose via the `Driver`'s associated `Transaction<'c>` type

If not supported, return relevant error messages in related functions and enable `disable-transactions` in `tank-tests`.

### 6. Test Certification
Add an integration test `tests/yourdb.rs`:

<<< @/../tank-yourdb/tests/yourdb.rs

Enable feature flags to disable specific functionality until green.

### Feature Flags
`tank-tests` exposes opt-out switches:
- `disable-arrays`, `disable-lists`, `disable-maps`: collections not implemented
- `disable-intervals`: interval types absent
- `disable-large-integers`: `i128`, `u128` unsupported
- `disable-ordering`: yourdb cannot order result sets
- `disable-references`: foreign keys not enforced
- `disable-transactions`: no transactional support

### 7. Tactical Checklist
- URL prefix enforced (`yourdb://`)
- `Driver::NAME` correct and used consistently
- `prepare` handles multiple statements (or rejects cleanly)
- Streams drop promptly (no leaked locks / file handles)
- `SqlWriter` prints multi‑statement sequences with proper separators and terminal `;`
- Upsert path (`save()`) works if PK exists; documented fallback if not supported

Remove a flag the moment your driver truly supports the capability. Each removed flag unlocks corresponding test sorties.

## Performance Notes
- Prefer streaming APIs over buffering entire result sets.
- Implement backend bulk ingestion if native (like DuckDB's appender) for `append()`.
- Reuse prepared statements internally if engine offers server‑side caching.

## Errors
Return early with rich context:
- Wrong URL prefix: immediate `Error::msg("YourDB connection url must start with `yourdb://`")`
- Prepare failure: attach truncated query text (`truncate_long!` style) to context
- Bind failure: specify parameter index and offending value type

*Forge the chassis. Calibrate the barrel. Roll new armor onto the field.*
