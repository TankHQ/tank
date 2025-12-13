# Advanced Operations
###### *Field Manual Section 7* - Tactical Coordination

In the field, isolated units rarely win the battle. Coordination is key. Joins let you link data across tables like synchronized squads advancing under fire.
In Tank, a join is a first class `DataSet`, just like a `TableRef`. That means you can call `select()` and then, filter, map, reduce, etc, using the same composable [Stream API](https://docs.rs/futures/latest/futures/prelude/trait.Stream.html) you already know.

## Schema In Play
Continuing with the `Operator` and `RadioLog` schema introduced earlier. The following examples show more advanced query capabilities-operations that go beyond simple CRUD while still avoiding resorting to raw SQL.
::: code-group
```rust [Rust]
#[derive(Entity)]
#[tank(schema = "operations", name = "radio_operator")]
pub struct Operator {
    #[tank(primary_key)]
    pub id: Uuid,
    pub callsign: String,
    #[tank(name = "rank")]
    pub service_rank: String,
    #[tank(name = "enlistment_date")]
    pub enlisted: Date,
    pub is_certified: bool,
}

#[derive(Entity)]
#[tank(schema = "operations")]
pub struct RadioLog {
    #[tank(primary_key)]
    pub id: Uuid,
    #[tank(references = Operator::id)]
    pub operator: Uuid,
    pub message: String,
    pub unit_callsign: String,
    #[tank(name = "tx_time")]
    pub transmission_time: OffsetDateTime,
    #[tank(name = "rssi")]
    pub signal_strength: i8,
}
```
```sql [SQL]
CREATE TABLE IF NOT EXISTS operations.radio_operator (
    id UUID PRIMARY KEY,
    callsign VARCHAR NOT NULL,
    rank VARCHAR NOT NULL,
    enlistment_date DATE NOT NULL,
    is_certified BOOLEAN NOT NULL);

CREATE TABLE IF NOT EXISTS operations.radio_log (
    id UUID PRIMARY KEY,
    operator UUID NOT NULL REFERENCES operations.radio_operator(id),
    message VARCHAR NOT NULL,
    unit_callsign VARCHAR NOT NULL,
    tx_time TIMESTAMP WITH TIME ZONE NOT NULL,
    rssi TINYINT NOT NULL);
```
:::

### Data
**Operators:**
| callsign    | rank  | enlisted   | is_certified |
| ----------- | ----- | ---------- | ------------ |
| SteelHammer | Major | 2015-06-20 | ✅ true      |
| Viper       | Sgt   | 2019-11-01 | ✅ true      |
| Rook        | Pvt   | 2023-01-15 | ❌ false     |

**Radio logs:**
| message                                  | unit_callsign | tx_time                | rssi |
| ---------------------------------------- | ------------- | ---------------------- | ---- |
| Radio check, channel 3. How copy?        | Alpha-1       | 2025-11-04T19:45:21+01 | −42  |
| Target acquired. Requesting coordinates. | Alpha-1       | 2025-11-04T19:54:12+01 | −55  |
| Heavy armor spotted, grid 4C.            | Alpha-1       | 2025-11-04T19:51:09+01 | −52  |
| Perimeter secure. All clear.             | Bravo-2       | 2025-11-04T19:51:09+01 | −68  |
| Radio check, grid 1A. Over.              | Charlie-3     | 2025-11-04T18:59:11+02 | −41  |
| Affirmative, engaging.                   | Alpha-1       | 2025-11-03T23:11:54+00 | −54  |

## Selecting & Ordering
The [`tank::cols!()`](https://docs.rs/tank/latest/tank/macro.cols.html) supports aliasing and ordering. When you only need raw columns prefer the terse array `[Operator::callsign, Operator::service_rank, Operator::enlisted]` or `Operator::columns()` syntax.

Objective: strongest certified transmissions excluding routine radio checks.
```rust
let messages = join!(Operator JOIN RadioLog ON Operator::id == RadioLog::operator)
    .select(
        executor,
        cols!(
            RadioLog::signal_strength as strength DESC,
            Operator::callsign ASC,
            RadioLog::message,
        ),
        // X != Y as LIKE => X NOT LIKE Y
        &expr!(Operator::is_certified && RadioLog::message != "Radio check%" as LIKE),
        Some(100),
    )
    .map(|row| {
        row.and_then(|row| {
            #[derive(Entity)]
            struct Row {
                message: String,
                callsign: String,
            }
            Row::from_row(row).and_then(|row| Ok((row.message, row.callsign)))
        })
    })
    .try_collect::<Vec<_>>()
    .await?;
assert!(
    messages.iter().map(|(a, b)| (a.as_str(), b.as_str())).eq([
        ("Heavy armor spotted, grid 4C.", "SteelHammer"),
        ("Affirmative, engaging.", "SteelHammer"),
        ("Target acquired. Requesting coordinates.", "SteelHammer"),
        ("Perimeter secure. All clear.", "Viper"),
    ]
    .into_iter())
);
```

## Expr
[`expr!()`](https://docs.rs/tank/latest/tank/macro.expr.html) is your tactical toolkit for crafting query expressions in Tank, blending Rust syntax with SQL semantics for precision strikes in database operations. It parses your input into an expression tree that drivers translate into backend-specific syntax. You can use it to specify conditions for fetch queries or column default values.

It accepts a subset of Rust syntax with additional sentinel tokens for SQL semantics:
- `42`, `1.2`, `"Alpha"`, `true`, `NULL`, `[1, 2, 3]` literal values
- `#value` variable evaluation
- `RadioLog::signal_strength` column reference
- `Operator::id == #some_uuid` comparison: `==`, `!=`, `>`, `>=`. `<`, `<=`
- `!Operator::is_certified || RadioLog::signal_strength < -20` logical: `&&`, `||`, `!`
- `(a + b) * (c - d)` math operations: `+`, `-`, `*`, `/`, `%`
- `(flags >> 1) & 3` bitwise operations: `|`, `&`, `<<`, `>>`
- `[1, 2, 3][0]` array or map indexing
- `alpha == ? && beta > ?` prepared statement parameters
- `COUNT(*)`, `SUM(RadioLog::signal_strength)` function calls and aggregates
- `1 as u128` type casting
- `PI` identifiers
- `col == NULL`, `col != NULL` null check, it becomes `IS NULL` / `IS NOT NULL`
- `value != "ab%" as LIKE` pattern matching, it becomes `value NOT LIKE 'ab%'` in sql, it also supports `IN`, `REGEXP` and `GLOB` (actual supports depends on the driver)
- `-(-PI) + 2 * (5 % (2 + 1)) == 7 && !(4 < 2)` combination of the previous

Parentheses obey standard Rust precedence. Empty invocation (`expr!()`) yields `false`. Ultimately, the drivers decide if and how these expressions are translated into the specific query language.

## Cols
[`tank::cols!()`](https://docs.rs/tank/latest/tank/macro.cols.html) builds a slice of column expressions (optionally ordered) suitable for a `SELECT` projection.
Each comma separated item becomes either a expression (parsed via [`expr!`](7-advanced-operations.html#expr)) or a ordered expression when followed by `ASC` or `DESC`.

Example of valid syntax
- `RadioLog::transmission_time`
- `Operator::service_rank as rank` aliasing
- `RadioLog::signal_strength + 10` expressions
- `AVG(RadioLog::signal_strength)` function calls
- `*` wildcard
- `COUNT(*)` counting
- `operations.radio_log.signal_strength.rssi` raw database identifier
- `Operator::enlisted DESC` ordering
- `AVG(ABS(Operator::enlisted - operations.radio_log.transmission_time)) as difference DESC` combination of the previous

## Performance notes
- Request only the necessary columns.
- Always prefer set a `limit` on the query when it makes sense.

*Units in position. Advance. Tank out.*
