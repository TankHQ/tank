use crate::{AsValue, Driver, Error, Prepared, Result, Value, truncate_long};
use std::{
    fmt::{self, Display},
    sync::Arc,
};

/// Executable query: raw SQL or prepared statement.
#[derive(Debug)]
pub enum Query<D: Driver> {
    /// Unprepared SQL text.
    Raw(String),
    /// Driver prepared statement.
    Prepared(D::Prepared),
}

impl<D: Driver> Query<D> {
    /// Returns `true` when this `Query` contains a backend-prepared statement.
    pub fn is_prepared(&self) -> bool {
        matches!(self, Query::Prepared(..))
    }
    /// Clear all bound values.
    pub fn clear_bindings(&mut self) -> Result<&mut Self> {
        let Self::Prepared(prepared) = self else {
            return Err(Error::msg("Cannot clear bindings of a raw query"));
        };
        prepared.clear_bindings()?;
        Ok(self)
    }
    /// Append a bound value.
    /// It results in an error if the query is not prepared.
    pub fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        let Self::Prepared(prepared) = self else {
            return Err(Error::msg("Cannot bind a raw query"));
        };
        prepared.bind(value)?;
        Ok(self)
    }
    /// Bind a value at a specific index.
    /// It results in an error if the query is not prepared.
    pub fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let Self::Prepared(prepared) = self else {
            return Err(Error::msg("Cannot bind index of a raw query"));
        };
        prepared.bind_index(value, index)?;
        Ok(self)
    }
}

pub trait AsQuery<D: Driver> {
    type Output: AsMut<Query<D>> + Send;
    fn as_query(self) -> Self::Output;
}

impl<D: Driver> AsQuery<D> for Query<D> {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        self
    }
}

impl<'q, D: Driver + 'q> AsQuery<D> for &'q mut Query<D> {
    type Output = &'q mut Query<D>;
    fn as_query(self) -> Self::Output {
        self
    }
}

impl<D: Driver> AsQuery<D> for String {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        Query::Raw(self)
    }
}

impl<D: Driver> AsQuery<D> for &str {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        Query::Raw(self.to_owned())
    }
}

impl<D: Driver> Default for Query<D> {
    fn default() -> Self {
        Self::Raw(Default::default())
    }
}

impl<D: Driver> From<&str> for Query<D> {
    fn from(value: &str) -> Self {
        Query::Raw(value.into())
    }
}

impl<D: Driver> From<String> for Query<D> {
    fn from(value: String) -> Self {
        Query::Raw(value.into())
    }
}

impl<D, P> From<P> for Query<D>
where
    D: Driver<Prepared = P>,
    P: Prepared,
{
    fn from(value: P) -> Self {
        Query::Prepared(value)
    }
}

impl<D: Driver> Display for Query<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Raw(query) => write!(f, "{}", truncate_long!(query)),
            Query::Prepared(query) => query.fmt(f),
        }
    }
}

impl<D: Driver> AsMut<Query<D>> for Query<D> {
    fn as_mut(&mut self) -> &mut Query<D> {
        self
    }
}

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

/// Items from `Executor::run`: rows or effects.
#[derive(Debug)]
pub enum QueryResult {
    /// A labeled row
    Row(RowLabeled),
    /// A modify effect aggregation
    Affected(RowsAffected),
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
