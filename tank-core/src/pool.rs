use crate::{
    AsQuery, Connection, Driver, Entity, Error, Executor, Query, QueryResult, Result, Row,
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

pub struct PooledConnection<D: Driver> {
    pub(crate) object: Object<DBConnectionManager<D>>,
}

pub trait ConnectionPool<D: Driver>: Debug {
    fn get<'s>(&'s self) -> BoxFuture<'s, Result<PooledConnection<D>>>;
    fn timeout_get<'s>(&'s self, timeout: Duration) -> BoxFuture<'s, Result<PooledConnection<D>>>;
    fn detach<'s>(&'s self) -> BoxFuture<'s, Result<D::Connection>>;
    fn resize(&self, max_size: usize) -> Result<()>;
    fn into_box(self) -> Box<dyn ConnectionPool<D>>
    where
        D: 'static;
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

    fn append<'a, E, It>(
        &mut self,
        entities: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        E: Entity + 'a,
        It: IntoIterator<Item = &'a E> + Send,
        <It as IntoIterator>::IntoIter: Send,
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
