# Tank Architecture & Performance Guide

This document records the core architectural invariants, ABI/LLVM optimization rules, and driver design patterns in `tank`. Read this before modifying `tank-core`, `tank-macros`, or any database driver.

## 1. Workspace & Toolchain

- **Crates**:
  - `tank`: Public facade re-exporting `tank-core` and `tank-macros`.
  - `tank-core`: Core traits (`Driver`, `Executor`, `Connection`, `Transaction`, `Entity`, `Row`, `AsValue`, `SqlWriter`), `Value<'a>`, and query builders.
  - `tank-macros`: Proc-macros for `#[derive(Entity)]`, `expr!`, `cols!`, and `join!`.
  - `tank-tests`: Shared integration test suite executed by every driver.
  - Drivers: `tank-postgres`, `tank-sqlite`, `tank-duckdb`, `tank-chdb`, `tank-clickhouse`, `tank-mysql`, `tank-mongodb`, `tank-scylladb`, `tank-valkey` (plus `tank-yourdb` template).
- **Toolchain**: Use `rustup run nightly-2026-10-12`.
- **Testing**: Run `./test.sh` (or `rustup run nightly-2026-10-12 cargo test -p <crate> --all-targets`).

## 2. Code Style & API Design Rules

1. **Minimal Public Surface**: Keep `Driver`, `Executor`, `Row`, `Entity`, and `AsValue` free of internal optimization hooks. Never expose driver-specific or macro-internal helper methods on public traits.
2. **Terse, Combinator-Oriented Code**: Prefer iterator/stream combinators and straightforward data flow over verbose state machines or extra helper types.
3. **Comment the *Why*, Not the *What***: Keep comments concise (1–3 lines) and reserve them for non-obvious invariants, ABI/LLVM SROA constraints, or protocol subtleties.

## 3. `Value<'a>` Layout & Memory Invariants (`tank-core/src/value/`)

- **`size_of::<Value>() == 32` and `align_of::<Value>() == 8`**:
  - Enforced by `tests/value.rs::value_layout_is_compact`.
  - Never add an unboxed variant to `Value<'a>` whose payload exceeds 24 bytes (or 16 bytes if it lacks a niche and requires 16-byte alignment).
  - Wide/rare variants are boxed:
    - `Int128(Option<Box<i128>>)` and `UInt128(Option<Box<u128>>)`
    - `Array(Option<Box<[Value<'a>]>>, Box<CollectionType<'a>>)`
    - `List(Option<Vec<Value<'a>>>, Box<Value<'a>>)`
    - `Map(Option<HashMap<Value<'a>, Value<'a>>>, Box<(Value<'a>, Value<'a>)>)`
    - `Struct(Option<Vec<(String, Value<'a>)>>, Box<StructType<'a>>)`
  - `Value::Json(Option<JsonValue>)` stores `Option<serde_json::Value>` inline (24 bytes via `serde_json::Value`'s niche) so JSON values do not require an extra `Box` allocation.
- **`Interval` Layout (`tank-core/src/value/interval.rs`)**:
  - `Interval` is 16 bytes (`nanos: i64`, `days: i32`, `months: i32` with `align == 8`), so `Option<Interval>` is 24 bytes and fits inline in `Value::Interval(Option<Interval>)`.
  - Arithmetic in `Interval` uses `i128` intermediates only during computation when `nanos` overflows `i64`.

## 4. LLVM SROA, `sret` Aliasing & Hot-Loop Decoding Rules

Because `size_of::<Value>() == 32` exceeds the two-register (16-byte) return limit of the x86-64 System V ABI, any non-inlined function returning `Value<'a>`, `Option<Cow<'_, Value<'_>>>`, or `Result<Value<'a>>` uses a hidden `sret` pointer (`ptr sret([32 x i8]) %_0`) on the stack.

To allow LLVM's Scalar Replacement of Aggregates (`SROA`) and `Mem2Reg` passes to keep `Value<'a>` entirely in SSA registers during `Entity::from_row` decoding:

1. **Keep Hot-Path `Value` Predicates `#[inline(always)]` and Consume `Value` by Value in Cold Error Helpers**:
   - When `ColumnDecoder::value` is inlined into `Entity::from_row`, a decoded `v: Value` does not exist in memory yet—its discriminant and payload live solely in CPU registers. Because CPU registers do not have a memory address, calling any **non-inlined** method taking `&Value` on the hot path (such as `v.is_null()` if it were not `#[inline(always)]`) forces LLVM to spill the register values into a 32-byte stack `alloca` just to form the `&Value` pointer.
   - In `AsValue::try_from_value`, the cold `Err` arm moves `v` **by value** into `#[cold] #[inline(never)] fn unexpected_value(value: Value<'_>, ...)` rather than formatting `{v:?}` inline:
     1. Moving `v` by value consumes ownership in the `Err` branch, eliminating `drop_in_place::<Value>(&mut v)` at function exit (which would otherwise take `&mut v` on the shared local slot).
     2. `rustc` materializes the outgoing argument slot only inside the cold `Err` basic block, so the `Ok` path keeps `v` in SSA registers with zero stack stores.
2. **Avoid `sret` Return-Slot Forwarding into Cold Functions**:
   - When an `#[inline(always)]` function returns a large type `T` (written via `sret`) and delegates its slow path to a `#[cold] #[inline(never)]` function with the **exact same return type `T`**, `rustc` forwards the caller's `sret` pointer directly into the cold call (`call cold_fn(ptr sret %0)`). LLVM `SROA` sees the `sret` pointer escape into an opaque call and refuses to promote the hot path's fields to registers.
   - **Solution**: Give the cold helper a slightly different return type so `rustc` allocates a separate temporary stack slot inside the cold branch:
     - `ColumnDecoder::value` (`tank-postgres/src/util.rs`) returns `Value<'a>`, while `ColumnDecoder::fallback` returns `Option<Value<'a>>` (unwrapped with `.unwrap_or(Value::Null)`).
     - `PostgresStream::poll_next` (`tank-postgres/src/rows.rs`) returns `Poll<Option<Result<T>>>`, while `PostgresStream::poll_init` returns `Poll<Result<Option<T>>>` (transposed in `poll_next`).
3. **Why `ColumnDecoder::Bool` Must Stay in `ColumnDecoder::value`'s Inline Switch (`tank-postgres/src/util.rs`)**:
   - In `Value<'a>`, byte `0..1` is the 1-byte `u8` tag and bytes `4..8` hold `Option<i32>`'s 4-byte discriminant (`Value::Int32`), leaving a 3-byte (`24`-bit) gap at bytes `1..4`.
   - Because `i24` is not a legal integer width in the x86-64 DataLayout (`n8:16:32:64`), if only `Int32` and `Text` are constructed inline, LLVM `SROA` creates an unpromotable `alloca [3 x i8]` for bytes `1..4` and emits `movzbl` + `movzwl` stack copies on every decoded column.
   - Including `ColumnDecoder::Bool` (`Value::Boolean(Option<bool>)`, which writes its 1-byte payload at offset `1..2`) in `ColumnDecoder::value` forces LLVM `SROA` to split `1..4` into `1..2` (`i8`) and `2..4` (`i16`), both legal widths that `Mem2Reg` promotes to SSA registers and dead-code-eliminates.
4. **Keep `ColumnDecoder::value` at 3 Inline Arms (`Bool`, `Int32`, `Text`)**:
   - LLVM x86 `SwitchLowering` lowers `< 4` switch cases to direct `cmp` + `je` branches (which the CPU branch predictor correlates per column position in the unrolled row decode loop).
   - Adding a 4th inline arm (`>= 4` cases) causes LLVM to emit an indirect jump table (`jmpq *(%rcx,%rax,8)`), which suffers indirect branch mispredictions when column types alternate across a row.
5. **Move Out of `Cow<Value>` via `&mut Value` + `mem::forget` (`tank-core/src/value/as_value.rs`, `tank-macros/src/from_row_trait.rs`)**:
   - Calling `Cow::into_owned()` on `Cow<'_, Value<'_>>` introduces a branch that clones borrowed `Value`s through the stack.
   - Instead, `impl AsValue for &mut Value<'a>` uses `mem::take(self)` (`Value` is `Default` -> `Value::Null`), and `#[derive(Entity)]` passes `&mut *value` to `AsValue::try_from_value`.
   - In `String::try_from_value` and `Cow<str>::try_from_value`, after allocating `s.to_owned()` from `Value::Varchar(Some(Cow::Borrowed(s)))`, call `mem::forget(v)`. Because `Cow::Borrowed` has no destructor, forgetting `v` eliminates the `drop_in_place::<Value>()` tag check without leaking memory.
   - In `Option<T>::try_from_value`, explicitly match `Value::Null | Value::Boolean(None) | ...` before delegating to `T::try_from_value(v)` so LLVM constant-folds the discriminant on inline `Value`s.

## 5. `#[derive(Entity)]` Row Decoding (`tank-macros/src/from_row_trait.rs`)

- **Positional Fast Path**: `from_row` first checks `if names == Self::column_names()`. When column names match the entity's declaration order (the common case for `Entity::find_many`, `Entity::find_one`, and ordered `SELECT` queries), columns are decoded directly by index `(0..N)` with zero per-row column name comparisons, and the loop is unrolled by the macro.
- **Byte-Slice Column Name Matching**: In the fallback path (arbitrary column order or subset), column names are matched via `match __n__.as_bytes() { [#(#name),*] => ... }` rather than `match __n__.as_str()`. Matching `&str` literals causes `rustc` to lower string patterns to `bcmp` with variable length before LLVM `SwitchConversion` runs, leaving `call bcmp@GLIBC` in the loop for non-power-of-two column name lengths (e.g. length 10). Byte-slice array patterns lower directly to a length check plus register-width integer loads (`movq` + `movzwl`).

## 6. Streaming & Zero-Copy Row Architecture (`tank-core/src/executor.rs`, `tank-postgres/src/rows.rs`)

- **`Executor::run` and `Executor::fetch` Return `impl Stream`**:
  - `Executor::run` returns `impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send` where `QueryResult<R>` is `QueryResult::Row(R) | QueryResult::Affected(RowsAffected)`.
  - `Executor::fetch` returns `impl Stream<Item = Result<impl Row + 's>> + Send`, defaulting to a lightweight `Fetched<S>` stream adapter over `self.run(query)` that filters out `QueryResult::Affected` without `async_stream!` overhead. Drivers like `tank-postgres` override `fetch` directly to skip wrapping rows in `QueryResult::Row`.
- **Borrowing Native Rows (`impl Row + 's`)**:
  - Returning `impl Row + 's` (bounded by the `&'s mut self` borrow of the executor) allows `PostgresRow<'s>` to borrow directly from `wtx`'s wire read buffer (`PostgresRecord<'s>`) and connection-level scratch (`PostgresScratch` holding `Arc<[String]>` column labels and `Vec<ColumnDecoder>`), avoiding per-row `Arc` clones, `Vec<ValueOwned>` allocations, and `String` copies for borrowed text/blobs.
- **Avoid `async_stream::try_stream!` on Hot Drivers**:
  - `try_stream!` allocates a channel-like sender/receiver state machine and polls a generator on every yielded item.
  - In `tank-postgres`, `PostgresStream<F, I>` awaits the query execution future once in an outlined `#[cold] #[inline(never)] fn poll_init` and then steps the inline iterator (`PostgresRows<'s>` or `PostgresResults<'s>`) directly in `poll_next`.
  - In drivers with very large multi-arm async generators (`tank-mongodb`), `.boxed()` is applied to the stream returned by `run` so the 12-arm generator state machine is heap-allocated and does not overflow thread stacks in unoptimized debug builds.
