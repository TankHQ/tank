use crate::{
    AsValue, Driver, DynQuery, Error, Prepared, QueryMetadata, Result, RowLabeled, RowsAffected,
    truncate_long,
};
use std::fmt::{self, Display};

#[derive(Default, Clone, Debug)]
pub struct RawQuery {
    pub sql: String,
    pub metadata: QueryMetadata,
}

impl Display for RawQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", truncate_long!(self.sql))
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
        Query::Raw(RawQuery {
            sql: value,
            metadata: Default::default(),
        })
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
    pub fn metadata(&self) -> &QueryMetadata {
        match self {
            Self::Raw(v) => &v.metadata,
            Self::Prepared(v) => v.metadata(),
        }
    }
    pub fn metadata_mut(&mut self) -> &mut QueryMetadata {
        match self {
            Self::Raw(v) => &mut v.metadata,
            Self::Prepared(v) => v.metadata_mut(),
        }
    }
    pub fn into_dyn(self) -> DynQuery {
        self.into()
    }
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
            Query::Raw(v) => v.fmt(f),
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
