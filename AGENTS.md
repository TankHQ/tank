# Tank Development Guide

Read before modifying `tank-core`, `tank-macros`, or database drivers.

## Workspace

- `tank`: Public facade re-exporting `tank-core` and `tank-macros`.
- `tank-core`: Traits (`Driver`, `Executor`, `Connection`, `Transaction`, `Entity`, `Row`, `AsValue`, `SqlWriter`), `Value<'a>`, and query builders.
- `tank-macros`: `#[derive(Entity)]`, `expr!`, `cols!`, and `join!` proc-macros.
- `tank-tests`: Shared integration suite run by every driver's `tests/` against `tank-core`.
- Drivers: `tank-postgres`, `tank-sqlite`, `tank-duckdb`, `tank-chdb`, `tank-clickhouse`, `tank-mysql`, `tank-mongodb`, `tank-scylladb`, `tank-valkey`. Template: `tank-yourdb`.

## Validation

- Toolchain: `rustup run nightly-2026-10-12`.
- Always run `./test.sh` after changes. It runs `cargo check`, then `cargo test -p <crate> --all-targets` independently for every workspace member, preserving each driver's `tank-tests` feature combination. Only this full run is the validation and coverage signal.
- Partial package tests or coverage runs cannot establish full-suite coverage. Workspace-wide commands such as `cargo test --workspace` may mask driver-specific configurations through cross-member feature resolution and do not replace `./test.sh`.
- Keep documentation snippets synchronized with their test copies in `tank-tests/src/`. Keep duplicated README content aligned with `docs/01-introduction.md` and `docs/02-getting-started.md`.

## Code Style & API

- Avoid panics in production code. Return errors for recoverable failures, including invalid input, decoding, I/O, and startup. Use `unwrap` or `expect` only when the call site guarantees success.
- Keep public traits (`Driver`, `Executor`, `Row`, `Entity`, `AsValue`) free of driver-specific, macro-internal, or optimization helpers.
- Put type-specific helpers and constants in the relevant `impl`, not at module scope, even when private. Only shared, type-independent functions belong in utility modules such as `util.rs`.
- Import types, traits, functions, and macros with top-level `use`, qualifying inline only to disambiguate collisions (e.g. `wtx::Error`). Test imports belong at file or enclosing `mod tests` scope, never inside tests. Import re-exports from `tank`, not `tank_core`, and do not mix both.
- Prefer iterator/stream combinators and direct data flow over verbose state machines or extra helper types.
- Comments explain non-obvious invariants, ABI/LLVM constraints, or protocol subtleties, not obvious behavior. Keep them to 1-3 lines.

## Value Layout (`tank-core/src/value/`)

- Preserve `size_of::<Value>() == 32` and `align_of::<Value>() == 8`, enforced by `tests/value.rs::value_layout_is_compact`. Unboxed payloads must not exceed 24 bytes, or 16 bytes when niche-free and requiring 16-byte alignment.
- Box wide/rare variants as follows: `Int128(Option<Box<i128>>)`, `UInt128(Option<Box<u128>>)`, `Array(Option<Box<[Value<'a>]>>, Box<CollectionType<'a>>)`, `List(Option<Vec<Value<'a>>>, Box<Value<'a>>)`, `Map(Option<HashMap<Value<'a>, Value<'a>>>, Box<(Value<'a>, Value<'a>)>)`, `Struct(Option<Vec<(String, Value<'a>)>>, Box<StructType<'a>>)`.
- Keep `Value::Json(Option<JsonValue>)` inline. `serde_json::Value`'s niche makes its `Option` 24 bytes, avoiding an extra allocation.
- In `interval.rs`, `Interval` is 16 bytes, aligned to 8 (`nanos: i64`, `days: i32`, `months: i32`). Its 24-byte `Option` fits inline in `Value::Interval`. Use `i128` arithmetic intermediates only when `nanos` overflows `i64`.

## Streams & Borrowing (`tank-core/src/executor.rs`, `tank-postgres/src/rows.rs`)

- `Executor::run` returns `impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send`, with `QueryResult::Row(R)` or `QueryResult::Affected(RowsAffected)`.
- `Executor::fetch` returns `impl Stream<Item = Result<impl Row + 's>> + Send`. Its default `Fetched<S>` adapter filters affected counts from `self.run(query)` without `async_stream!`. PostgreSQL overrides it to avoid `QueryResult::Row` wrapping.
- Bound rows to the executor's `&'s mut self` borrow. `PostgresRow<'s>` borrows `wtx`'s wire buffer (`PostgresRecord<'s>`) and connection-cached `RowLabels` (`Arc<[String]>`), avoiding per-row `Arc` clones, `Vec<ValueOwned>` allocations, and borrowed text/blob copies.
- Avoid `async_stream::try_stream!` in hot drivers: its channel-like sender/receiver state machine polls a generator per item. `PostgresStream<F, I>` awaits query execution once in `#[cold] #[inline(never)] poll_init`, then steps `PostgresRows<'s>` or `PostgresResults<'s>` inline in `poll_next`.
- Box very large generators: `tank-mongodb` applies `.boxed()` to `run`'s 12-arm stream, moving its state to the heap to avoid debug-build thread-stack overflow.
- Preserve `try_collect` size hints (`tank-core/src/lib.rs`). Futures' version calls `items.extend(iter::once(item))` with `(1, Some(1))` per item, ignoring stream hints and causing about 15 `Vec` reallocations for 10,000 rows. `tank_core::stream::TryStreamExt::try_collect` drains through `ReadyItems`, forwarding `Iterator::size_hint` to `self.stream.size_hint()`. After `poll_init` initializes `PostgresRows` on the first item, `Vec::spec_extend` reserves the exact remaining count in one allocation.

## Entity & Value Conversion

- **Positional decoding** (`tank-macros/src/from_row_trait.rs`): First check `names == Self::column_names()`. Matching declaration order (usual for `find_many`, `find_one`, and ordered `SELECT`s) uses macro-unrolled indices `0..N`, avoiding per-column name comparisons on each row.
- **Name matching** (same file): For arbitrary column order/subsets, match byte-slice arrays (`match __n__.as_bytes() { [#(#name),*] => ... }`), not `as_str()` literals. String patterns become variable-length `bcmp` before LLVM SwitchConversion, leaving `bcmp@GLIBC` calls for non-power-of-two lengths (e.g. 10). Byte patterns become length checks and register-width loads (`movq` + `movzwl`).
- **Ownership** (`tank-core/src/value/as_value.rs`): Use `mem::take(self)` for `AsValue for &mut Value<'a>` (`Value::default()` is `Null`). The derive macro passes `&mut *value` to `AsValue::try_from_value`. Avoid `Cow::into_owned()`, which adds a borrowed-value cloning branch through the stack.
- **Borrowed strings**: In `String::try_from_value` and `Cow<str>::try_from_value`, call `mem::forget(v)` after `s.to_owned()` from `Value::Varchar(Some(Cow::Borrowed(s)))`. This removes the `drop_in_place::<Value>()` tag check without leaking, since `Cow::Borrowed` has no destructor.
- **Optional values** (`tank-core/src/as_value.rs`): Keep distinct branch targets: `!same` calls `fallback(&mut other)`, while `same && val.is_null()` returns `Ok(None)` directly. Retain separate `if same` and `if val.is_null()` checks. Combining them as `if same && !val.is_null()` lets SimplifyCFG hoist the null check ahead of `same`, exceeding JumpThreading's duplication threshold and blocking jump-threading of `PostgresRow::value`'s inline arms through the conversion.

## PostgreSQL & LLVM Optimization

`Value` exceeds the x86-64 System V ABI's two-register, 16-byte return limit. Non-inlined returns of `Value<'a>`, `Option<Cow<'_, Value<'_>>>`, and `Result<Value<'a>>` use a hidden stack `sret` pointer (`ptr sret([32 x i8]) %_0`). Preserve these rules so Scalar Replacement of Aggregates (SROA) and Mem2Reg keep `Entity::from_row` decoding in SSA registers. PostgreSQL code is in `tank-postgres/src/rows.rs`.

- **Inline predicates, move cold errors.** Keep hot `Value` predicates such as `is_null` `#[inline(always)]`. A non-inlined `&Value` call forces a 32-byte stack spill of the inlined `PostgresRow::value` result. In `AsValue::try_from_value`, move `v` into `#[cold] #[inline(never)] fn unexpected_value(value: Value<'_>, ...)`, rather than formatting `{v:?}` inline. Consuming it removes exit-time `drop_in_place::<Value>(&mut v)` on the shared slot, and confines outgoing argument storage to `Err`, leaving `Ok` store-free.
- **Give cold helpers different return types.** If an `#[inline(always)]` large-type return delegates to a `#[cold] #[inline(never)]` helper returning the same type, rustc forwards the caller's `sret` slot into the opaque call, preventing SROA. A different type confines the temporary to the cold branch. Keep `PostgresRow::value -> Option<Cow<'_, Value<'_>>>` versus `decode_fallback -> Option<Value<'a>>`, and `PostgresStream::poll_next -> Poll<Option<Result<T>>>` versus `poll_init -> Poll<Result<()>>`.
- **Keep `Ty::Bool` inline in `PostgresRow::value`.** `Value`'s `u8` tag occupies `0..1`, and `Int32`'s `Option<i32>` discriminant occupies `4..8`. With only `Int32` and `Varchar` inline, the gap at `1..4` becomes an unpromotable `alloca [3 x i8]`: x86-64's `n8:16:32:64` DataLayout has no legal `i24`, causing per-column `movzbl` + `movzwl` stack copies. `Value::Boolean(Option<bool>)` writes at `1..2`, splitting the gap into legal `i8` and `i16` slices that Mem2Reg promotes and eliminates.
- **Delegate decoding to `wtx::codec::Decode`.** Inspect `*dw.ty()` on the `PostgresDecodeWrapper` from `self.record.value(index)`. Use `Decode<Postgres<wtx::Error>>` for `bool`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, `Decimal`, `Uuid`, `&str`, and `&[u8]`, not a separate `ColumnDecoder` array or duplicate byte parsing. The inline `Ty::Bool`, `Ty::Int4`, and `Ty::Varchar | Ty::Text` error arms use `mem::forget(e)` to remove hot-path `wtx::Error` drop glue: these decoders return only stack-only `DatabaseError::UnexpectedBufferSize` or `Error::InvalidUTF8` variants.
