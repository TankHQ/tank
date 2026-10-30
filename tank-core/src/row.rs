use crate::{Value, ValueOwned};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

/// Shared column labels.
pub type RowLabels = Arc<[String]>;

/// A labeled row, either materialized or decoded from a native driver view.
pub trait Row: Debug + Send + Sync {
    /// Column names in result order.
    fn names(&self) -> &[String];

    /// Number of columns.
    fn len(&self) -> usize;

    /// Borrows a stored value or returns a freshly decoded value.
    /// SQL NULL is a `Value`; `None` means no value at this index.
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_column(&self, name: &str) -> Option<Cow<'_, Value<'_>>> {
        self.names()
            .iter()
            .position(|label| label == name)
            .and_then(|index| self.value(index))
    }

    /// Iterates values without cloning stored payloads, stopping at a missing cell.
    fn values(&self) -> impl Iterator<Item = Cow<'_, Value<'_>>> {
        (0..self.len()).map_while(|index| self.value(index))
    }

    fn iter(&self) -> impl Iterator<Item = (&str, Cow<'_, Value<'_>>)> {
        self.names().iter().map(String::as_str).zip(self.values())
    }

    /// Copies names and owns all borrowed value payloads.
    fn into_owned(self) -> RowOwned
    where
        Self: Sized,
    {
        RowOwned::new(
            self.names().into(),
            self.values()
                .map(|value| value.into_owned().into_owned())
                .collect(),
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
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        (**self).value(index)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    #[inline(always)]
    fn get_column(&self, name: &str) -> Option<Cow<'_, Value<'_>>> {
        (**self).get_column(name)
    }

    #[inline(always)]
    fn values(&self) -> impl Iterator<Item = Cow<'_, Value<'_>>> {
        (**self).values()
    }

    #[inline(always)]
    fn iter(&self) -> impl Iterator<Item = (&str, Cow<'_, Value<'_>>)> {
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
    pub fn new(names: RowLabels, values: Vec<ValueOwned>) -> Self {
        Self { names, values }
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
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        self.values.get(index).map(Cow::Borrowed)
    }

    #[inline(always)]
    fn into_owned(self) -> RowOwned {
        self
    }
}
