use crate::{QueryResult, Value};
use std::sync::Arc;

/// Metadata about modifying operations (INSERT/UPDATE/DELETE).
#[derive(Default, Debug, Clone, Copy)]
pub struct RowsAffected {
    /// Optional count of affected rows reported by the backend.
    /// `None` means the backend did not provide a count.
    pub rows_affected: Option<u64>,
    /// Optional last affected identifier (driver-dependent meaning).
    /// For many drivers this is the last inserted id.
    pub last_affected_id: Option<i64>,
}

/// Shared reference-counted column name list.
pub type RowNames = Arc<[String]>;
/// Owned row value slice matching `RowNames` length.
pub type Row = Box<[Value]>;

/// Row with column labels.
#[derive(Debug, Clone)]
pub struct RowLabeled {
    /// Column names
    pub labels: RowNames,
    /// Values aligned with labels
    pub values: Row,
}

impl RowLabeled {
    pub fn new(names: RowNames, values: Row) -> Self {
        Self {
            labels: names,
            values,
        }
    }
    /// Returns the column labels for this row.
    pub fn names(&self) -> &[String] {
        &self.labels
    }
    /// Returns the values associated with `names()`.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
    /// Look up a column value by its label name.
    pub fn get_column(&self, name: &str) -> Option<&Value> {
        self.labels
            .iter()
            .position(|v| v == name)
            .map(|i| &self.values()[i])
    }
}

impl Extend<RowsAffected> for RowsAffected {
    fn extend<T: IntoIterator<Item = RowsAffected>>(&mut self, iter: T) {
        for elem in iter {
            if self.rows_affected.is_some() || elem.rows_affected.is_some() {
                self.rows_affected = Some(
                    self.rows_affected.unwrap_or_default() + elem.rows_affected.unwrap_or_default(),
                );
            }
            if elem.last_affected_id.is_some() {
                self.last_affected_id = elem.last_affected_id;
            }
        }
    }
}

impl From<RowLabeled> for Row {
    fn from(value: RowLabeled) -> Self {
        value.values
    }
}

impl<'a> From<&'a RowLabeled> for &'a Row {
    fn from(value: &'a RowLabeled) -> Self {
        &value.values
    }
}

impl From<RowLabeled> for QueryResult {
    fn from(value: RowLabeled) -> Self {
        QueryResult::Row(value)
    }
}

impl From<RowsAffected> for QueryResult {
    fn from(value: RowsAffected) -> Self {
        QueryResult::Affected(value)
    }
}
