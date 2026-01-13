use crate::{
    AsValue, Driver, Error, Prepared, Result, RowLabeled, RowsAffected, TableRef, truncate_long,
};
use std::fmt::{self, Display, Write};

#[derive(Default, Debug)]
pub struct RawQuery {
    value: String,
    table: TableRef,
}

impl RawQuery {
    pub fn new(value: String) -> Self {
        Self {
            value,
            table: Default::default(),
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(String::with_capacity(capacity))
    }
    pub fn buffer(&mut self) -> &mut String {
        &mut self.value
    }
    pub fn as_str(&self) -> &str {
        &self.value
    }
    pub fn with_table(mut self, table: TableRef) -> Self {
        self.table = table;
        self
    }
    pub fn table(&self) -> &TableRef {
        &self.table
    }
    pub fn table_mut(&mut self) -> &mut TableRef {
        &mut self.table
    }
    pub fn push_str(&mut self, s: &str) {
        self.value.push_str(s);
    }
    pub fn push(&mut self, c: char) {
        self.value.push(c);
    }
    pub fn len(&self) -> usize {
        self.value.len()
    }
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

impl Write for RawQuery {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s);
        Ok(())
    }
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.push(c);
        Ok(())
    }
}

/// Executable query: raw SQL or prepared statement.
#[derive(Debug)]
pub enum Query<D: Driver> {
    /// Unprepared SQL text.
    Raw(RawQuery),
    /// Driver prepared statement.
    Prepared(D::Prepared),
}

impl<D: Driver> Query<D> {
    /// Create a raw query
    pub fn raw(value: String) -> Self {
        Query::Raw(RawQuery::new(value))
    }
    /// Create a prepared query
    pub fn prepared(value: D::Prepared) -> Self {
        Query::Prepared(value)
    }
    /// Returns `true` when this `Query` contains a backend-prepared statement.
    pub fn is_prepared(&self) -> bool {
        matches!(self, Query::Prepared(..))
    }
    /// Clear all bound values.
    pub fn clear_bindings(&mut self) -> Result<&mut Self> {
        if let Self::Prepared(prepared) = self {
            prepared.clear_bindings()?;
        };
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
    pub fn with_table(mut self, table: TableRef) -> Self {
        self = match self {
            Query::Raw(v) => Query::Raw(v.with_table(table)),
            Query::Prepared(v) => Query::Prepared(v.with_table(table)),
        };
        self
    }
    pub fn table(&self) -> &TableRef {
        match self {
            Query::Raw(v) => v.table(),
            Query::Prepared(v) => v.table(),
        }
    }
}

pub trait QueryMetadata {
    fn with_table(self, table: TableRef) -> Self;
    fn table(&self) -> &TableRef;
}

impl<D: Driver> Default for Query<D> {
    fn default() -> Self {
        Self::raw(Default::default())
    }
}

impl<D: Driver> From<&str> for Query<D> {
    fn from(value: &str) -> Self {
        Self::raw(value.into())
    }
}

impl<D: Driver> From<String> for Query<D> {
    fn from(value: String) -> Self {
        Self::raw(value)
    }
}

impl<D, P> From<P> for Query<D>
where
    D: Driver<Prepared = P>,
    P: Prepared,
{
    fn from(value: P) -> Self {
        Self::prepared(value)
    }
}

impl<D: Driver> Display for Query<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Raw(query) => write!(f, "{}", truncate_long!(query.value)),
            Query::Prepared(query) => query.fmt(f),
        }
    }
}

impl<D: Driver> AsMut<Query<D>> for Query<D> {
    fn as_mut(&mut self) -> &mut Query<D> {
        self
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
