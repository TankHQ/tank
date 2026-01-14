use crate::{AsValue, QueryMetadata, Result, TableRef};
use std::fmt::{Debug, Display};

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
pub trait Prepared: Send + Sync + Display + Debug {
    /// Clear all bound values.
    fn clear_bindings(&mut self) -> Result<&mut Self>;
    /// Append a bound value.
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self>;
    /// Bind a value at a specific index.
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self>;
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
