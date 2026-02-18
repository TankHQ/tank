use crate::{AsValue, Result};
use std::{
    any::Any,
    fmt::{Debug, Display},
};

/// Parameterized, backend-prepared query handle.
///
/// Enables pre-parsing and parameter binding.
///
/// # Semantics
/// * `bind`: Append value.
/// * `bind_index`: Set value at 0-based index.
pub trait Prepared: Any + Send + Sync + Display + Debug {
    fn as_any(self: Box<Self>) -> Box<dyn Any>;
    /// Clear all bindings.
    fn clear_bindings(&mut self) -> Result<&mut Self>
    where
        Self: Sized;
    /// Bind next value.
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>
    where
        Self: Sized;
    /// Bind value at index.
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self>
    where
        Self: Sized;
    /// True if the query has no meaningful value (the meaning depends on the driver).
    fn is_empty(&self) -> bool {
        false
    }
}
