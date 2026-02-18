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

/// Shared column names.
pub type RowNames = Arc<[String]>;
/// Row values matching `RowNames`.
pub type Row = Box<[Value]>;

/// Row with column labels.
#[derive(Default, Clone, Debug)]
pub struct RowLabeled {
    /// Column names.
    pub labels: RowNames,
    /// Column values.
    pub values: Row,
}

impl RowLabeled {
    pub fn new(names: RowNames, values: Row) -> Self {
        Self {
            labels: names,
            values,
        }
    }
    /// Column labels.
    pub fn names(&self) -> &[String] {
        &self.labels
    }
    /// Row values.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
    /// Get value by column name.
    pub fn get_column(&self, name: &str) -> Option<&Value> {
        self.labels
            .iter()
            .position(|v| v == name)
            .map(|i| &self.values()[i])
    }
    /// Column count.
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl<'s> IntoIterator for &'s RowLabeled {
    type Item = (&'s String, &'s Value);
    type IntoIter = iter::Zip<slice::Iter<'s, String>, slice::Iter<'s, Value>>;
    fn into_iter(self) -> Self::IntoIter {
        iter::zip(self.labels.iter(), self.values.iter())
    }
}

impl<'s> IntoIterator for &'s mut RowLabeled {
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
