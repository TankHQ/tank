use crate::{QueryResult, Value, ValueOwned};
use std::{
    fmt::{self, Debug, Formatter},
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

/// Shared column labels.
pub type RowLabels = Arc<[String]>;
/// Row values matching `RowLabels`.
pub type RowValues = Vec<ValueOwned>;

/// A labeled row, either materialized or decoded from a native driver view.
pub trait Row: Debug + Send + Sync {
    /// Column names in result order.
    fn names(&self) -> &[String];

    /// Number of columns.
    fn len(&self) -> usize;

    /// Borrows a stored value or returns a freshly decoded value.
    /// SQL NULL is a `Value`; `None` means no value at this index.
    fn value(&self, index: usize) -> Option<Value<'_>>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_column(&self, name: &str) -> Option<Value<'_>> {
        self.names()
            .iter()
            .position(|label| label == name)
            .and_then(|index| self.value(index))
    }

    /// Iterates values without cloning stored payloads, stopping at a missing cell.
    fn values(&self) -> impl Iterator<Item = Value<'_>> {
        (0..self.len()).map_while(|index| self.value(index))
    }

    fn take_values(self) -> impl Iterator<Item = ValueOwned>;

    fn iter(&self) -> impl Iterator<Item = (&str, Value<'_>)> {
        self.names().iter().map(String::as_str).zip(self.values())
    }

    /// Copies names and owns all borrowed value payloads.
    fn into_owned(self) -> RowOwned
    where
        Self: Sized,
    {
        RowOwned::new(
            self.names().into(),
            self.take_values()
                .map(|value| value.into_owned().into_owned())
                .collect::<Vec<_>>(),
        )
    }
}

impl<R: Row + ?Sized> Row for &R {
    #[inline(always)]
    fn names(&self) -> &[String] {
        (**self).names()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        (**self).len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Value<'_>> {
        (**self).value(index)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    #[inline(always)]
    fn get_column(&self, name: &str) -> Option<Value<'_>> {
        (**self).get_column(name)
    }

    #[inline(always)]
    fn values(&self) -> impl Iterator<Item = Value<'_>> {
        (**self).values()
    }

    #[inline(always)]
    fn take_values(self) -> impl Iterator<Item = ValueOwned> {
        (*self).values().map(|v| v.clone().into_owned())
    }

    #[inline(always)]
    fn iter(&self) -> impl Iterator<Item = (&str, Value<'_>)> {
        (**self).iter()
    }
}

/// An owning row with shared column names.
#[derive(Clone, Default)]
pub struct RowOwned {
    pub names: RowLabels,
    pub values: Vec<ValueOwned>,
}

impl RowOwned {
    pub fn new(names: RowLabels, values: impl Into<Vec<ValueOwned>>) -> Self {
        Self {
            names,
            values: values.into(),
        }
    }
}

impl Debug for RowOwned {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowOwned")
            .field("names", &self.names)
            .field("len", &self.values.len())
            .finish()
    }
}

impl Row for RowOwned {
    #[inline(always)]
    fn names(&self) -> &[String] {
        &self.names
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Value<'_>> {
        self.values.get(index).cloned()
    }

    fn take_values(self) -> impl Iterator<Item = ValueOwned> {
        self.values.into_iter()
    }

    #[inline(always)]
    fn into_owned(self) -> RowOwned {
        self
    }
}

impl<R: Row> From<R> for QueryResult<R> {
    fn from(value: R) -> Self {
        QueryResult::Row(value)
    }
}

impl<R> From<RowsAffected> for QueryResult<R> {
    fn from(value: RowsAffected) -> Self {
        QueryResult::Affected(value)
    }
}
