use crate::{QueryResult, Value};
use std::{
    borrow::Cow,
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
///
/// A `Cow` so a driver can avoid further memory allocation.
pub type RowValues<'a> = Cow<'a, [Value<'a>]>;
/// Owning row values.
pub type RowValuesOwned = RowValues<'static>;

/// Row with column labels.
#[derive(Default, Clone, Debug)]
pub struct Row<'a> {
    /// Shared reference to column names for efficient storage.
    pub labels: RowLabels,
    /// Data values for this specific row.
    pub values: RowValues<'a>,
}

/// Owning row.
pub type RowOwned = Row<'static>;

impl<'a> Row<'a> {
    /// Creates a new row from labels and values.
    pub fn new(names: RowLabels, values: RowValues<'a>) -> Self {
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
    pub fn values(&self) -> &[Value<'a>] {
        &self.values
    }
    /// Retrieves a value by its column name. Returns `None` if the column doesn't exist.
    pub fn get_column(&self, name: &str) -> Option<&Value<'a>> {
        self.labels
            .iter()
            .position(|v| v == name)
            .and_then(|i| self.values.get(i))
    }
    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }
    /// Converts this row into an owning [`RowOwned`], cloning any borrowed payload.
    pub fn into_owned(self) -> RowOwned {
        Row {
            labels: self.labels,
            values: Cow::Owned(
                self.values
                    .into_owned()
                    .into_iter()
                    .map(Value::into_owned)
                    .collect(),
            ),
        }
    }
}

impl<'s, 'a> IntoIterator for &'s Row<'a> {
    type Item = (&'s String, &'s Value<'a>);
    type IntoIter = iter::Zip<slice::Iter<'s, String>, slice::Iter<'s, Value<'a>>>;
    fn into_iter(self) -> Self::IntoIter {
        iter::zip(self.labels.iter(), self.values.iter())
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

impl<'a> From<Row<'a>> for RowValues<'a> {
    fn from(value: Row<'a>) -> Self {
        value.values
    }
}

impl<'a> From<&'a Row<'a>> for &'a RowValues<'a> {
    fn from(value: &'a Row<'a>) -> Self {
        &value.values
    }
}

impl<'a> From<Row<'a>> for QueryResult<'a> {
    fn from(value: Row<'a>) -> Self {
        QueryResult::Row(value)
    }
}

impl<'a> From<RowsAffected> for QueryResult<'a> {
    fn from(value: RowsAffected) -> Self {
        QueryResult::Affected(value)
    }
}
