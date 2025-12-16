use crate::{AsValue, Result};
use std::fmt::{Debug, Display};

/// A parameterized, backend-prepared query handle.
///
/// `Prepared` enables drivers to pre-parse / optimize SQL statements and later
/// bind positional parameters. Values are converted via the `AsValue` trait.
///
/// # Binding Semantics
/// * `bind` appends a value (driver chooses actual placeholder numbering).
/// * `bind_index` sets the parameter at `index` (from 0).
///
/// Methods return `&mut Self` for fluent chaining:
/// ```rust, ignore
/// prepared.bind(42)?.bind("hello")?;
/// ```
pub trait Prepared: Send + Sync + Display + Debug {
    /// Clear all bound values.
    fn clear_bindings(&mut self) -> Result<&mut Self>;
    /// Append a bound value.
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>;
    /// Bind a value at a specific index.
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self>;
}
