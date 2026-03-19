use crate::{QueryResult, Value};
use std::{
    iter::{self},
    slice,
    sync::Arc,
};

/// Result of a modifying operation (INSERT/UPDATE/DELETE).
#[derive(Default, Clone, Copy, Debug)]
pub struct RowsAffected {
    /// Number of rows modified (if supported by backend).
    pub rows_affected: Option<u64>,
    /// Last inserted ID (driver-dependent).
    pub last_affected_id: Option<i64>,
}

/// Shared columns labels.
pub type RowLabels = Arc<[String]>;
/// Row values matching `RowLabels`.
pub type RowValues = Box<[Value]>;

/// Row with column labels.
#[derive(Default, Clone, Debug)]
pub struct Row {
    /// Shared reference to column names for efficient storage.
    pub labels: RowLabels,
    /// Data values for this specific row.
    pub values: RowValues,
}

impl Row {
    /// Creates a new row from labels and values.
    pub fn new(names: RowLabels, values: RowValues) -> Self {
        Self {
            labels: names,
            values,
        }
    }
    /// Accesses the column names associated with this row.
    pub fn names(&self) -> &[String] {
        &self.labels
    }
    /// Accesses the raw values in this row.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
    /// Retrieves a value by its column name. Returns `None` if the column doesn't exist.
    pub fn get_column(&self, name: &str) -> Option<&Value> {
        self.labels
            .iter()
            .position(|v| v == name)
            .map(|i| &self.values()[i])
    }
    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl<'s> IntoIterator for &'s Row {
    type Item = (&'s String, &'s Value);
    type IntoIter = iter::Zip<slice::Iter<'s, String>, slice::Iter<'s, Value>>;
    fn into_iter(self) -> Self::IntoIter {
        iter::zip(self.labels.iter(), self.values.iter())
    }
}

impl<'s> IntoIterator for &'s mut Row {
    type Item = (&'s String, &'s mut Value);
    type IntoIter = iter::Zip<slice::Iter<'s, String>, slice::IterMut<'s, Value>>;
    fn into_iter(self) -> Self::IntoIter {
        iter::zip(self.labels.iter(), self.values.iter_mut())
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

impl From<Row> for RowValues {
    fn from(value: Row) -> Self {
        value.values
    }
}

impl<'a> From<&'a Row> for &'a RowValues {
    fn from(value: &'a Row) -> Self {
        &value.values
    }
}

impl From<Row> for QueryResult {
    fn from(value: Row) -> Self {
        QueryResult::Row(value)
    }
}

impl From<RowsAffected> for QueryResult {
    fn from(value: RowsAffected) -> Self {
        QueryResult::Affected(value)
    }
}
