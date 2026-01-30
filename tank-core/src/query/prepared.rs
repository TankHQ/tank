use crate::{AsValue, Result};
use std::{
    any::Any,
    fmt::{Debug, Display},
};

/// A parameterized, backend-prepared query handle.
///
/// `Prepared` enables drivers to pre-parse / optimize SQL statements and later
/// bind positional parameters. Values are converted via the `AsValue` trait.
///
/// # Binding Semantics
/// * `bind` appends a value (driver chooses actual placeholder numbering).
/// * `bind_index` sets the parameter at `index` (zero-based).
///
/// Methods return `&mut Self` for fluent chaining:
/// ```ignore
/// prepared.bind(42)?.bind("hello")?;
/// ```
pub trait Prepared: Any + Send + Sync + Display + Debug {
    fn as_any(self: Box<Self>) -> Box<dyn Any>;
    /// Clear all bound values.
    fn clear_bindings(&mut self) -> Result<&mut Self>
    where
        Self: Sized;
    /// Append a bound value.
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>
    where
        Self: Sized;
    /// Bind a value at a specific index.
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self>
    where
        Self: Sized;
    /// Returns true if self has no meaningfull value. The meaning depends on the driver.
    fn is_empty(&self) -> bool {
        false
    }
}
