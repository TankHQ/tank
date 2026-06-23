use crate::{Connection, Driver, Error, Result};
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult, Timeouts};
use std::{borrow::Cow, fmt::Debug, future, time::Duration};

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
        Ok(Box::pin(D::Connection::connect(&self.driver, self.url.clone())).await?)
    }
    fn recycle(
        &self,
        _: &mut Self::Type,
        _: &Metrics,
    ) -> impl Future<Output = RecycleResult<Self::Error>> + Send {
        future::ready(RecycleResult::Ok(()))
    }
}

pub trait ConnectionPool<D: Driver>: Debug {
    fn get(
        &self,
    ) -> impl Future<Output = Result<impl AsRef<D::Connection> + AsMut<D::Connection>>> + Send;
    fn timeout_get(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<impl AsRef<D::Connection> + AsMut<D::Connection>>> + Send;
    fn detach(&self) -> impl Future<Output = Result<D::Connection>> + Send
    where
        Self: Sized;
    fn resize(&self, max_size: usize) -> Result<()>;
    fn close(self) -> impl Future<Output = Result<()>> + Send;
}

impl<D: Driver> ConnectionPool<D> for Pool<DBConnectionManager<D>>
where
    <D as Driver>::Connection: Debug,
{
    async fn get(&self) -> Result<impl AsRef<D::Connection> + AsMut<D::Connection>> {
        Ok(Pool::<DBConnectionManager<D>>::get(self)
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?)
    }

    async fn timeout_get(
        &self,
        timeout: Duration,
    ) -> Result<impl AsRef<D::Connection> + AsMut<D::Connection>> {
        Ok(Pool::<DBConnectionManager<D>>::timeout_get(
            self,
            &Timeouts::wait_millis(timeout.as_millis() as u64),
        )
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?)
    }

    async fn detach(&self) -> Result<D::Connection>
    where
        Self: Sized,
    {
        let v = Pool::<DBConnectionManager<D>>::get(self)
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?;
        Ok(Object::<DBConnectionManager<D>>::take(v))
    }

    fn resize(&self, max_size: usize) -> Result<()> {
        Ok(self.resize(max_size))
    }

    fn close(self) -> impl Future<Output = Result<()>> + Send {
        Self::close(&self);
        future::ready(Ok(()))
    }
}
