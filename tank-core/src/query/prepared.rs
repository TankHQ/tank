use crate::{AsValue, QueryMetadata, Result, TableRef};
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
    /// Get QueryMetadata
    fn metadata(&self) -> &QueryMetadata;
    /// Get mutable QueryMetadata
    fn metadata_mut(&mut self) -> &mut QueryMetadata;
    /// Getter for the query results limit, if it exists
    fn get_limit(&self) -> Option<u32> {
        self.metadata().limit
    }
    /// Table and schema this query targets. The values (schema / table / alias) can also be empty.
    fn get_table(&self) -> &TableRef {
        &self.metadata().table
    }
    /// Mutable table and schema this query targets. The values (schema / table / alias) can also be empty.
    fn get_table_mut(&mut self) -> &mut TableRef {
        &mut self.metadata_mut().table
    }
    /// Replace the target table from this query
    fn with_table(mut self, table: TableRef) -> Self
    where
        Self: Sized,
    {
        self.metadata_mut().table = table;
        self
    }
}
