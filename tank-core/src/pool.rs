use crate::{
    AsEntity, AsQuery, Connection, Driver, Error, Executor, Query, QueryResult, Result, Row,
    RowsAffected,
};
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult, Timeouts};
use futures::{FutureExt, Stream, future::BoxFuture};
use std::{
    borrow::Cow,
    fmt::Debug,
    future,
    ops::{Deref, DerefMut},
    time::Duration,
};

/// The [`Manager`] that backs every Tank connection pool.
///
/// A manager holds the [`Driver`] and the database URL; it knows how to open
/// new connections on demand and how to validate recycled ones.  You do not
/// need to construct or interact with this type directly, it is created
/// internally by [`Driver::connect_pool`].
#[derive(Debug)]
pub struct DBConnectionManager<D: Driver> {
    driver: D,
    url: Cow<'static, str>,
}

impl<D: Driver> DBConnectionManager<D> {
    pub fn new(driver: D, url: Cow<'static, str>) -> Self {
        Self { driver, url }
    }
}

impl<D: Driver> Manager for DBConnectionManager<D> {
    type Type = D::Connection;
    type Error = Error;
    async fn create(&self) -> Result<Self::Type> {
        Ok(D::Connection::connect(&self.driver, self.url.clone()).await?)
    }
    fn recycle(
        &self,
        _: &mut Self::Type,
        _: &Metrics,
    ) -> impl Future<Output = RecycleResult<Self::Error>> + Send {
        future::ready(RecycleResult::Ok(()))
    }
}

/// A database connection borrowed from a [`ConnectionPool`].
///
/// Implements both [`Executor`] and [`Connection`], so it can be used anywhere
/// a plain connection is expected.  Deref-coerces to the underlying driver
/// connection (`D::Connection`) via [`Deref`]/[`DerefMut`], giving access to
/// any driver-specific extension methods.
///
/// The borrowed connection is automatically returned to the pool when this
/// value is dropped. If you need to take full ownership of the connection
/// outside pool management, call [`ConnectionPool::detach`] instead.
#[derive(Debug)]
pub struct PooledConnection<D: Driver> {
    pub(crate) object: Object<DBConnectionManager<D>>,
}

/// A managed pool of reusable database connections.
///
/// Every method that yields connections produces a [`PooledConnection`]
/// that is automatically returned to the pool on drop, keeping the number
/// of open database connections bounded.
pub trait ConnectionPool<D: Driver>: Debug {
    /// Acquires a connection from the pool.
    ///
    /// If all connections are in use and the pool is at its maximum size, this
    /// call waits until one becomes available or the configured
    /// [`PoolConfig::wait_timeout`] elapses, at which point an error is
    /// returned.
    fn get<'s>(&'s self) -> BoxFuture<'s, Result<PooledConnection<D>>>;

    /// Acquires a connection from the pool with an explicit timeout.
    ///
    /// Identical to [`get`](ConnectionPool::get) but overrides the configured
    /// wait timeout with `timeout`.  Useful when individual call sites need
    /// stricter or looser deadlines than the pool-level default.
    fn timeout_get<'s>(&'s self, timeout: Duration) -> BoxFuture<'s, Result<PooledConnection<D>>>;

    /// Acquires a connection and removes it from pool management.
    ///
    /// The returned `D::Connection` is a plain, pool-unaware connection.  It
    /// will **not** be returned to the pool when dropped, the underlying
    /// database connection is closed instead. Use this when you need to hand
    /// a connection to code that does not know about the pool, or when you
    /// want to guarantee the connection is fully closed rather than recycled.
    fn detach<'s>(&'s self) -> BoxFuture<'s, Result<D::Connection>>;

    /// Changes the maximum number of connections the pool may hold.
    fn resize(&self, max_size: usize) -> Result<()>;

    /// Converts this pool into a sized type-erased `Box<dyn ConnectionPool<D>>`.
    ///
    /// `Driver::connect_pool` returns an opaque `impl ConnectionPool<D>` type
    /// that cannot be named or stored in struct fields, returned from trait
    /// implementations, or otherwise used where a concrete named type is required.
    fn into_box(self) -> Box<dyn ConnectionPool<D>>
    where
        D: 'static;

    /// Closes the pool, marking it as unavailable and dropping all managed
    /// connections.
    fn close(self) -> BoxFuture<'static, Result<()>>;
}

impl<D: Driver> ConnectionPool<D> for Pool<DBConnectionManager<D>>
where
    <D as Driver>::Connection: Debug,
{
    fn get<'s>(&'s self) -> BoxFuture<'s, Result<PooledConnection<D>>> {
        async move {
            let object = Pool::<DBConnectionManager<D>>::get(self)
                .await
                .map_err(|e| Error::msg(format!("{e:#?}")))?;
            Ok(PooledConnection { object })
        }
        .boxed()
    }

    fn timeout_get<'s>(&'s self, timeout: Duration) -> BoxFuture<'s, Result<PooledConnection<D>>> {
        async move {
            let object = Pool::<DBConnectionManager<D>>::timeout_get(
                self,
                &Timeouts::wait_millis(timeout.as_millis() as u64),
            )
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?;
            Ok(PooledConnection::<D> { object })
        }
        .boxed()
    }

    fn detach<'s>(&'s self) -> BoxFuture<'s, Result<D::Connection>>
    where
        Self: Sized,
    {
        async {
            let v = Pool::<DBConnectionManager<D>>::get(self)
                .await
                .map_err(|e| Error::msg(format!("{e:#?}")))?;
            Ok(Object::<DBConnectionManager<D>>::take(v))
        }
        .boxed()
    }

    fn resize(&self, max_size: usize) -> Result<()> {
        Ok(self.resize(max_size))
    }

    fn into_box(self) -> Box<dyn ConnectionPool<D>>
    where
        D: 'static,
    {
        Box::new(self)
    }

    fn close(self) -> BoxFuture<'static, Result<()>> {
        Self::close(&self);
        future::ready(Ok(())).boxed()
    }
}

impl<D: Driver> Executor for PooledConnection<D> {
    type Driver = D;

    fn accepts_multiple_statements(&self) -> bool {
        self.object.accepts_multiple_statements()
    }

    fn driver(&self) -> D {
        self.object.driver()
    }

    fn prepare<'s>(
        &'s mut self,
        query: impl AsQuery<D> + 's,
    ) -> impl Future<Output = Result<Query<D>>> + Send {
        self.object.prepare(query)
    }

    fn do_prepare(&mut self, sql: String) -> impl Future<Output = Result<Query<D>>> + Send {
        self.object.do_prepare(sql)
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<D> + 's,
    ) -> impl Stream<Item = Result<QueryResult>> + Send {
        self.object.run(query)
    }

    fn fetch<'s>(
        &'s mut self,
        query: impl AsQuery<D> + 's,
    ) -> impl Stream<Item = Result<Row>> + Send {
        self.object.fetch(query)
    }

    fn execute<'s>(
        &'s mut self,
        query: impl AsQuery<D> + 's,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        self.object.execute(query)
    }

    fn append<It>(&mut self, entities: It) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        self.object.append(entities)
    }
}

impl<D: Driver> Connection for PooledConnection<D> {
    fn connect(
        _driver: &D,
        _url: Cow<'static, str>,
    ) -> impl Future<Output = Result<<D as Driver>::Connection>> + Send
    where
        Self: Sized,
    {
        future::ready(Err(Error::msg(
            "Cannot connect using a PooledConnection, such object must be obtained from a connection pool",
        )))
    }

    fn begin(&mut self) -> impl Future<Output = Result<<D as Driver>::Transaction<'_>>> + Send {
        self.object.begin()
    }
}

impl<D: Driver> Deref for PooledConnection<D> {
    type Target = D::Connection;
    fn deref(&self) -> &Self::Target {
        &self.object
    }
}

impl<D: Driver> DerefMut for PooledConnection<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.object
    }
}

impl<D: Driver> AsRef<D::Connection> for PooledConnection<D> {
    fn as_ref(&self) -> &D::Connection {
        &self.object
    }
}

impl<D: Driver> AsMut<D::Connection> for PooledConnection<D> {
    fn as_mut(&mut self) -> &mut D::Connection {
        &mut self.object
    }
}
