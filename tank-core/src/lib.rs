//! Async, portable SQL toolkit for Rust.
//!
//! - Define entities with `#[derive(Entity)]`.
//! - Build expressions (`expr!`, `cols!`, `join!`).
//! - Execute with any driver via `Executor`.
//! - Typed values via `Value`/`AsValue`.
//!
//! All fallible APIs use `tank::Result<T>`. Futures/streams do nothing until awaited.
mod as_value;
mod column;
mod connection;
mod data_set;
mod decode_type;
mod driver;
mod entity;
mod executor;
mod expression;
mod interval;
mod join;
mod prepared;
mod query;
mod relations;
mod table_ref;
mod transaction;
mod util;
mod value;
mod writer;

pub use ::anyhow::Context as ErrorContext;
pub use as_value::*;
pub use column::*;
pub use connection::*;
pub use data_set::*;
pub use decode_type::*;
pub use driver::*;
pub use entity::*;
pub use executor::*;
pub use expression::*;
pub use interval::*;
pub use join::*;
pub use prepared::*;
pub use query::*;
pub use relations::*;
pub use table_ref::*;
pub use transaction::*;
pub use util::*;
pub use value::*;
pub use writer::*;
pub mod stream {
    pub use ::futures::stream::*;
}
pub use ::futures::future;
pub use ::futures::sink;

/// Crate-wide result alias using `anyhow` for flexible error context.
pub type Result<T> = anyhow::Result<T>;
/// Crate-wide error alias using `anyhow`.
pub type Error = anyhow::Error;

pub use ::indoc;
