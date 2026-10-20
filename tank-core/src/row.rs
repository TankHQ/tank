use crate::{QueryResult, Value};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
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

/// A driver-provided source of row values that are decoded on demand.
///
/// Drivers whose result set live in a network buffer
/// can implement this and expose a lazy [`RowValues`].
pub trait RowValuesSource: Send + Sync {
    /// Number of values in the nth `row`.
    fn row_len(&self, row: usize) -> usize;
    /// Decodes the nth `col` value of the nth `row`.
    ///
    /// `None` is returned for a SQL NULL (or an out-of-range index).
    fn value(&self, row: usize, col: usize) -> Option<Value<'_>>;
}

/// Marker [`RowValuesSource`] for rows that are already materialised.
pub enum NoSource {}

impl RowValuesSource for NoSource {
    #[inline]
    fn row_len(&self, _row: usize) -> usize {
        0
    }
    #[inline]
    fn value(&self, _row: usize, _col: usize) -> Option<Value<'_>> {
        None
    }
}

/// Row values matching `RowLabels`.
///
/// Either materialised (`Owned`) or decoded on demand from a driver source (`Lazy`).
pub enum RowValues<'a, S: RowValuesSource + ?Sized> {
    /// Materialised values.
    Owned(Cow<'a, [Value<'a>]>),
    /// Values decoded on demand from a driver source.
    Lazy {
        /// The driver source.
        source: &'a S,
        /// Index of this row within the source.
        row: usize,
    },
}

/// Owning row values.
pub type RowValuesOwned = RowValues<'static, NoSource>;

impl<S: RowValuesSource + ?Sized> Clone for RowValues<'_, S> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(v) => Self::Owned(v.clone()),
            Self::Lazy { source, row } => Self::Lazy {
                source: *source,
                row: *row,
            },
        }
    }
}

impl<S: RowValuesSource + ?Sized> Default for RowValues<'_, S> {
    fn default() -> Self {
        Self::Owned(Cow::Owned(Vec::new()))
    }
}

impl<S: RowValuesSource + ?Sized> Debug for RowValues<'_, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowValues")
            .field("len", &self.len())
            .finish()
    }
}

impl<'a, S: RowValuesSource + ?Sized> RowValues<'a, S> {
    /// Number of values.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(v) => v.len(),
            Self::Lazy { source, row } => source.row_len(*row),
        }
    }

    /// True if there are no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the `index`-th value, decoding it on demand for a lazy source.
    #[inline]
    pub fn get(&self, index: usize) -> Option<Value<'a>> {
        match self {
            Self::Owned(v) => v.get(index).cloned(),
            Self::Lazy { source, row } => source.value(*row, index),
        }
    }

    /// Iterates the values, decoding lazily.
    #[inline]
    pub fn iter(&self) -> RowValuesIter<'_, 'a, S> {
        match self {
            Self::Owned(v) => RowValuesIter::Owned(v.iter()),
            Self::Lazy { source, row } => RowValuesIter::Lazy {
                source,
                row: *row,
                col: 0,
                len: source.row_len(*row),
            },
        }
    }

    /// Materialises the values, cloning any borrowed payload.
    pub fn into_owned(self) -> RowValuesOwned {
        match self {
            Self::Owned(v) => RowValuesOwned::Owned(Cow::Owned(
                v.into_owned().into_iter().map(Value::into_owned).collect(),
            )),
            Self::Lazy { source, row } => RowValuesOwned::Owned(Cow::Owned(
                (0..source.row_len(row))
                    .filter_map(|i| source.value(row, i))
                    .map(Value::into_owned)
                    .collect(),
            )),
        }
    }

    /// Re-tags a materialised value set with a different source type.
    ///
    /// Only valid for [`RowValues::Owned`]; a lazy value cannot change source.
    pub fn cast_source<S2: RowValuesSource + ?Sized>(self) -> RowValues<'a, S2> {
        match self {
            Self::Owned(v) => RowValues::Owned(v),
            Self::Lazy { .. } => {
                unreachable!("a lazy RowValues cannot be re-tagged with another source")
            }
        }
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<Cow<'a, [Value<'a>]>> for RowValues<'a, S> {
    fn from(value: Cow<'a, [Value<'a>]>) -> Self {
        Self::Owned(value)
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<Vec<Value<'a>>> for RowValues<'a, S> {
    fn from(value: Vec<Value<'a>>) -> Self {
        Self::Owned(Cow::Owned(value))
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<&'a [Value<'a>]> for RowValues<'a, S> {
    fn from(value: &'a [Value<'a>]) -> Self {
        Self::Owned(Cow::Borrowed(value))
    }
}

impl<'a, S: RowValuesSource + ?Sized, const N: usize> From<[Value<'a>; N]> for RowValues<'a, S> {
    fn from(value: [Value<'a>; N]) -> Self {
        Self::Owned(Cow::Owned(value.into_iter().collect()))
    }
}

/// Iterator over the values of a [`RowValues`].
///
/// An enum rather than a `Box<dyn Iterator>` so both the owning and the lazy
/// case stay monomorphic and allocation-free. The owning case is a plain slice
/// iterator; the lazy case decodes one cell per `next`.
pub enum RowValuesIter<'s, 'a, S: RowValuesSource + ?Sized> {
    /// Iterates a materialised slice.
    Owned(slice::Iter<'s, Value<'a>>),
    /// Decodes on demand from a driver source.
    Lazy {
        source: &'a S,
        row: usize,
        col: usize,
        len: usize,
    },
}

impl<'s, 'a, S: RowValuesSource + ?Sized> Iterator for RowValuesIter<'s, 'a, S> {
    type Item = Value<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Owned(iter) => iter.next().cloned(),
            Self::Lazy {
                source,
                row,
                col,
                len,
            } => {
                if *col >= *len {
                    return None;
                }
                let value = source.value(*row, *col);
                *col += 1;
                value
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::Owned(iter) => iter.len(),
            Self::Lazy { col, len, .. } => len.saturating_sub(*col),
        };
        (remaining, Some(remaining))
    }
}

impl<'s, 'a, S: RowValuesSource + ?Sized> ExactSizeIterator for RowValuesIter<'s, 'a, S> {}

/// Row with column labels.
pub struct Row<'a, S: RowValuesSource + ?Sized> {
    /// Shared reference to column names for efficient storage.
    pub labels: RowLabels,
    /// Data values for this specific row.
    pub values: RowValues<'a, S>,
}

/// Owning row.
pub type RowOwned = Row<'static, NoSource>;

impl<S: RowValuesSource + ?Sized> Clone for Row<'_, S> {
    fn clone(&self) -> Self {
        Self {
            labels: self.labels.clone(),
            values: self.values.clone(),
        }
    }
}

impl<S: RowValuesSource + ?Sized> Debug for Row<'_, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("labels", &self.labels)
            .field("values", &self.values)
            .finish()
    }
}

impl<S: RowValuesSource + ?Sized> Default for Row<'_, S> {
    fn default() -> Self {
        Self {
            labels: RowLabels::default(),
            values: RowValues::default(),
        }
    }
}

impl<'a, S: RowValuesSource + ?Sized> Row<'a, S> {
    /// Creates a new row from labels and values.
    pub fn new(names: RowLabels, values: RowValues<'a, S>) -> Self {
        Self {
            labels: names,
            values,
        }
    }
    /// Accesses the column names associated with this row.
    pub fn names(&self) -> &[String] {
        &self.labels
    }
    /// Iterates the values of this row, decoding lazily.
    #[inline]
    pub fn values(&self) -> RowValuesIter<'_, 'a, S> {
        self.values.iter()
    }
    /// Retrieves a value by its column name. Returns `None` if the column doesn't exist.
    pub fn get_column(&self, name: &str) -> Option<Value<'a>> {
        self.labels
            .iter()
            .position(|v| v == name)
            .and_then(|i| self.values.get(i))
    }
    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }
    /// True if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    /// Re-tags a materialised row with a different source type.
    pub fn cast_source<S2: RowValuesSource + ?Sized>(self) -> Row<'a, S2> {
        Row {
            labels: self.labels,
            values: self.values.cast_source(),
        }
    }
    /// Converts this row into an owning [`RowOwned`], cloning any borrowed payload.
    pub fn into_owned(self) -> RowOwned {
        Row {
            labels: self.labels,
            values: self.values.into_owned(),
        }
    }
}

/// Iterator over the labeled values of a [`Row`].
pub struct RowIter<'s, 'a, S: RowValuesSource + ?Sized> {
    labels: slice::Iter<'s, String>,
    values: RowValuesIter<'s, 'a, S>,
}

impl<'s, 'a, S: RowValuesSource + ?Sized> Iterator for RowIter<'s, 'a, S> {
    type Item = (&'s String, Value<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let label = self.labels.next()?;
        let value = self.values.next()?;
        Some((label, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}

impl<'s, 'a, S: RowValuesSource + ?Sized> IntoIterator for &'s Row<'a, S> {
    type Item = (&'s String, Value<'a>);
    type IntoIter = RowIter<'s, 'a, S>;
    fn into_iter(self) -> Self::IntoIter {
        RowIter {
            labels: self.labels.iter(),
            values: self.values.iter(),
        }
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

impl<'a, S: RowValuesSource + ?Sized> From<Row<'a, S>> for RowValues<'a, S> {
    fn from(value: Row<'a, S>) -> Self {
        value.values
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<&'a Row<'a, S>> for &'a RowValues<'a, S> {
    fn from(value: &'a Row<'a, S>) -> &'a RowValues<'a, S> {
        &value.values
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<Row<'a, S>> for QueryResult<'a, S> {
    fn from(value: Row<'a, S>) -> Self {
        QueryResult::Row(value)
    }
}

impl<'a, S: RowValuesSource + ?Sized> From<RowsAffected> for QueryResult<'a, S> {
    fn from(value: RowsAffected) -> Self {
        QueryResult::Affected(value)
    }
}
