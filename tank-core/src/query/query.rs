use crate::{AsValue, Driver, DynQuery, Prepared, Result, Row, RowOwned, truncate_long};
use anyhow::anyhow;
use std::fmt::{self, Debug, Display};

/// Byte span of a parameter marker inside a built query
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryParam {
    pub begin: usize,
    pub end: usize, // exclusive
}

#[derive(Default, Clone, Debug)]
pub struct RawQuery {
    pub sql: String,
    pub params: Vec<QueryParam>,
}

impl RawQuery {
    pub fn new(sql: String) -> Self {
        Self {
            sql,
            params: Vec::new(),
        }
    }
    pub fn with_params(sql: String, params: Vec<QueryParam>) -> Self {
        Self { sql, params }
    }
    pub fn clear(&mut self) {
        self.sql.clear();
        self.params.clear();
    }
}

impl From<String> for RawQuery {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<RawQuery> for String {
    fn from(value: RawQuery) -> Self {
        value.sql
    }
}

impl Display for RawQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", truncate_long!(self.sql))
    }
}

/// Executable query: raw SQL or prepared statement.
#[derive(Debug)]
pub enum Query<D: Driver> {
    /// Raw SQL text.
    Raw(RawQuery),
    /// Prepared statement.
    Prepared(D::Prepared),
}

impl<D: Driver> Query<D> {
    /// New raw query.
    pub fn raw(value: String) -> Self {
        Query::Raw(value.into())
    }
    /// New prepared query.
    pub fn prepared(value: D::Prepared) -> Self {
        Query::Prepared(value)
    }
    /// True if it is the prepared variant.
    pub fn is_prepared(&self) -> bool {
        matches!(self, Query::Prepared(..))
    }
    /// Clear bound values.
    pub fn clear_bindings(&mut self) -> Result<&mut Self> {
        if let Self::Prepared(prepared) = self {
            prepared.clear_bindings()?;
        };
        Ok(self)
    }
    /// Bind value to next placeholder.
    ///
    /// Error if not prepared.
    pub fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        let Self::Prepared(prepared) = self else {
            return Err(anyhow!("Cannot bind a raw query"));
        };
        prepared.bind(value)?;
        Ok(self)
    }
    /// Bind value at index.
    ///
    /// Error if not prepared.
    pub fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let Self::Prepared(prepared) = self else {
            return Err(anyhow!("Cannot bind index of a raw query"));
        };
        prepared.bind_index(value, index)?;
        Ok(self)
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
            Query::Raw(v) => Display::fmt(v, f),
            Query::Prepared(query) => Display::fmt(query, f),
        }
    }
}

impl<D: Driver> AsMut<Query<D>> for Query<D> {
    fn as_mut(&mut self) -> &mut Query<D> {
        self
    }
}

/// Result of a modifying operation.
#[derive(Default, Clone, Copy, Debug)]
pub struct RowsAffected {
    /// Number of rows modified (if supported by backend).
    pub rows_affected: Option<u64>,
    /// Last inserted or modified ID (driver-dependent).
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

/// Items from `Executor::run`: rows or effects.
#[derive(Debug)]
pub enum QueryResult<R = RowOwned> {
    /// A labeled row.
    Row(R),
    /// A modify effect aggregation.
    Affected(RowsAffected),
}

/// Owning query result.
pub type QueryResultOwned = QueryResult<RowOwned>;

impl<R: Row> From<R> for QueryResult<R> {
    fn from(value: R) -> Self {
        Self::Row(value)
    }
}

impl<R> From<RowsAffected> for QueryResult<R> {
    fn from(value: RowsAffected) -> Self {
        Self::Affected(value)
    }
}
