# Advanced Operations
###### *Field Manual Section 8* - Tactical Coordination

In the field, isolated units rarely win the battle. Coordination is key. Joins let you link data across tables like synchronized squads advancing under fire.
In Tank, a join is a first-class `Dataset`, just like a `TableRef`. Pass it to `.from()` of a `Sql().select(...)` query, fetch the selected rows, then process them with the composable [Stream API](https://docs.rs/futures/latest/futures/prelude/trait.Stream.html) you already know.

## Schema
Continuing with the `Operator` and `RadioLog` schema introduced earlier. The following examples show more advanced query capabilities: operations that go beyond simple CRUD while still avoiding raw SQL.

The SQL below uses PostgreSQL syntax. Other drivers choose their own column types.
::: code-group
<<< @/../tank-tests/src/operations.rs#operations-schema [Rust]
```sql [SQL]
CREATE TABLE IF NOT EXISTS operations.radio_operator (
    id UUID PRIMARY KEY,
    callsign TEXT NOT NULL,
    rank TEXT NOT NULL,
    enlistment_date DATE NOT NULL,
    is_certified BOOLEAN NOT NULL);

CREATE TABLE IF NOT EXISTS operations.radio_log (
    id UUID PRIMARY KEY,
    operator UUID NOT NULL REFERENCES operations.radio_operator(id),
    message TEXT NOT NULL,
    unit_callsign TEXT NOT NULL,
    tx_time TIMESTAMP WITH TIME ZONE NOT NULL,
    rssi SMALLINT NOT NULL);
```
:::

## Data
**Operators:**
| callsign    | rank  | enlisted   | is_certified |
| ----------- | ----- | ---------- | ------------ |
| SteelHammer | Major | 2015-06-20 | ✅ true      |
| Viper       | Sgt   | 2019-11-01 | ✅ true      |
| Rook        | Pvt   | 2023-01-15 | ❌ false     |

**Radio logs:**
| message                                  | unit_callsign | tx_time                | rssi |
| ---------------------------------------- | ------------- | ---------------------- | ---- |
| Radio check, channel 3. How copy?        | Alpha-1       | 2025-11-04T19:45:21+01 | -42  |
| Target acquired. Requesting coordinates. | Alpha-1       | 2025-11-04T19:54:12+01 | -55  |
| Heavy armor spotted, grid 4C.            | Alpha-1       | 2025-11-04T19:51:09+01 | -52  |
| Perimeter secure. All clear.             | Bravo-2       | 2025-11-04T19:51:09+01 | -68  |
| Radio check, grid 1A. Over.              | Charlie-3     | 2025-11-04T18:59:11+02 | -41  |
| Affirmative, engaging.                   | Alpha-1       | 2025-11-03T23:11:54+00 | -54  |

## Selecting & Ordering
The [`tank::cols!()`](https://docs.rs/tank/latest/tank/macro.cols.html) macro supports aliasing and ordering. When you only need raw columns, prefer the terse array `[Operator::callsign, Operator::service_rank, Operator::enlisted]` or `Operator::columns()` syntax.

Objective: strongest certified transmissions excluding routine radio checks.

<<< @/../tank-tests/src/operations.rs#operations-select-order

## Expr
[`expr!()`](https://docs.rs/tank/latest/tank/macro.expr.html) parses a Rust-like expression into a typed AST that drivers translate into backend-specific queries. Use it for conditions or default values.

It accepts a subset of Rust syntax with additional sentinel tokens for SQL semantics:
- `42`, `1.2`, `"Alpha"`, `true`, `NULL`, `[1, 2, 3]` literal values
- `#value` variable evaluation
- `RadioLog::signal_strength` column reference
- `Operator::id == #some_uuid` comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`
- `!Operator::is_certified || RadioLog::signal_strength < -20` logical: `&&`, `||`, `!`
- `(a + b) * (c - d)` math operations: `+`, `-`, `*`, `/`, `%`
- `(flags >> 1) & 3` bitwise operations: `|`, `&`, `<<`, `>>`
- `[1, 2, 3][0]` array or map indexing
- `alpha == ? && beta > ?` prepared statement parameters
- `COUNT(*)`, `SUM(RadioLog::signal_strength)` function calls and aggregates
- `Operator::id as op_id` aliasing (column renaming)
- `PI` identifiers
- `col == NULL`, `col != NULL` null check, becomes `IS NULL`/`IS NOT NULL`
- `value != "ab%" as LIKE` pattern matching: becomes `value NOT LIKE 'ab%'` in SQL. Also supports `IN`, `REGEXP`, and `GLOB` (actual support depends on the driver)
- `Operator::callsign == #signs as IN`, becomes: `callsign IN ('Alpha', 'Bravo-2')`
- `-(-PI) + 2 * (5 % (2 + 1)) == 7 && !(4 < 2)` combination of the previous
- `CAST((2 > 1) as i32)` casting expression (mind the parentheses), the type names are automatically converted by the driver
- `if RadioLog::signal_strength > -20 { "strong" } else { "weak" }` conditional: becomes `CASE WHEN signal_strength > -20 THEN 'strong' ELSE 'weak' END`, `else if`
- `match Operator::service_rank { 1 => "recruit", 2 | 3 => "veteran", _ => "officer" }` conditional: becomes `CASE service_rank WHEN 1 THEN 'recruit' WHEN 2 THEN 'veteran' WHEN 3 THEN 'veteran' ELSE 'officer' END`.
- `current_timestamp_ms!()` milliseconds since epoch. Each driver renders it in its own dialect: `CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT)`, `(unixepoch('subsec') * 1000)`, `epoch_ms(current_timestamp)`, ...

Parentheses obey standard Rust precedence. Empty invocation (`expr!()`) yields `false`. Ultimately, the drivers decide if and how these expressions are translated into the specific query language.

> [!NOTE]
> The Rust cast expression (`as`) can mean different things depending on context.
> Consider: `CAST((4 == (2, 3, 4, 5) as IN) as i64) as read`.
> Here, `as` has three different meanings:
> 1. Declares that `4 == (2, 3, 4, 5)` is actually `4 IN (2, 3, 4, 5)`.
> 2. Separates the cast target type from the expression being cast.
> 3. Gives a name to the resulting column (when used in a `SELECT`).

> [!WARNING]
> **Conditionals** (`if` and `match`) are not supported by:
> - ScyllaDB / Cassandra: CQL has no conditional expressions, the database rejects the query.
> - Valkey / Redis: the driver logs an error and does not render the expression.

## Cols
[`tank::cols!()`](https://docs.rs/tank/latest/tank/macro.cols.html) builds a slice of projection expressions (optionally ordered). Each item is an expression (parsed via [`expr!`](#expr)) or an ordered expression when followed by `ASC` or `DESC`.

Example of valid syntax
- `RadioLog::transmission_time`
- `Operator::service_rank as rank` aliasing
- `RadioLog::signal_strength + 10` expressions
- `AVG(RadioLog::signal_strength)` function calls
- `*` wildcard
- `COUNT(*)` counting
- `operations.radio_log.rssi` raw database identifier
- `Operator::enlisted DESC` ordering
- `AVG(ABS(Operator::enlisted - operations.radio_log.tx_time)) as difference DESC` combination of the previous

## Performance Notes
- Request only the necessary columns.
- Always prefer setting a `limit` on the query when it makes sense.

*Units in position. Advance. Tank out.*
