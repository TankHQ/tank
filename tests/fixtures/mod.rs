use std::{
    any::Any,
    borrow::Cow,
    fmt,
    future::{self, Future},
};
use tank::{
    AsQuery, AsValue, Connection, Driver, Executor, GenericSqlWriter, Prepared, QueryResult,
    Result, Row, RowOwned, Transaction,
    stream::{self, Stream},
};

/// Minimal driver that renders SQL with [`GenericSqlWriter`] and never connects.
#[derive(Clone, Debug, Default)]
pub struct GenericDriver;

pub const DRIVER: GenericDriver = GenericDriver;

impl fmt::Display for GenericDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("generic")
    }
}

impl Driver for GenericDriver {
    type Connection = Self;
    type SqlWriter = GenericSqlWriter;
    type Prepared = Self;
    type Transaction<'c> = Self;
    const NAME: &'static [&'static str] = &["generic"];

    fn sql_writer(&self) -> Self::SqlWriter {
        GenericSqlWriter::new()
    }
}

impl Prepared for GenericDriver {
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        Ok(self)
    }

    fn bind(&mut self, _: impl AsValue) -> Result<&mut Self> {
        Ok(self)
    }

    fn bind_index(&mut self, _: impl AsValue, _: u64) -> Result<&mut Self> {
        Ok(self)
    }

    fn is_empty(&self) -> bool {
        true
    }
}

impl Executor for GenericDriver {
    type Driver = Self;

    fn run<'s>(
        &'s mut self,
        _: impl AsQuery<Self::Driver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
        stream::iter(std::iter::empty::<Result<QueryResult<RowOwned>>>())
    }
}

impl Connection for GenericDriver {
    fn connect(
        _: &Self::Driver,
        _: Cow<'static, str>,
    ) -> impl Future<Output = Result<Self>> + Send {
        future::ready(Ok(Self))
    }

    fn begin(&mut self) -> impl Future<Output = Result<Self>> + Send {
        future::ready(Ok(Self))
    }
}

impl Transaction<'_> for GenericDriver {
    fn commit(self) -> impl Future<Output = Result<()>> + Send {
        future::ready(Ok(()))
    }

    fn rollback(self) -> impl Future<Output = Result<()>> + Send {
        future::ready(Ok(()))
    }
}
