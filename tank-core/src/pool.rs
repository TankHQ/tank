use crate::{Connection, Driver, Error, Result};
use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult, Timeouts};
use std::{future, ops::Deref, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub struct DBConnectionManager<C: Connection>(Arc<Mutex<C>>);

impl<C: Connection> DBConnectionManager<C> {
    pub fn new(connection: C) -> Self {
        Self(Arc::new(Mutex::const_new(connection)))
    }
}

impl<C: Connection> Deref for DBConnectionManager<C> {
    type Target = <Arc<Mutex<C>> as Deref>::Target;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<C: Connection> Manager for DBConnectionManager<C>
where
    <C::Driver as Driver>::Connection: Into<C>,
{
    type Type = C;
    type Error = Error;
    async fn create(&self) -> Result<Self::Type> {
        Ok(self.lock().await.duplicate().await?.into())
    }
    fn recycle(
        &self,
        _: &mut Self::Type,
        _: &Metrics,
    ) -> impl Future<Output = RecycleResult<Self::Error>> + Send {
        future::ready(RecycleResult::Ok(()))
    }
}

pub trait ConnectionPool<C: Connection>
where
    <C::Driver as Driver>::Connection: Into<C>,
{
    fn get(&self) -> impl Future<Output = Result<impl AsRef<C> + AsMut<C>>> + Send;
    fn timeout_get(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<impl AsRef<C> + AsMut<C>>> + Send;
    fn detach(&self) -> impl Future<Output = Result<C>> + Send
    where
        Self: Sized;
    fn resize(&self, max_size: usize) -> Result<()>;
}

impl<C: Connection> ConnectionPool<C> for Pool<DBConnectionManager<C>>
where
    <C::Driver as Driver>::Connection: Into<C>,
{
    async fn get(&self) -> Result<impl AsRef<C> + AsMut<C>> {
        Ok(self
            .get()
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?)
    }

    async fn timeout_get(&self, timeout: Duration) -> Result<impl AsRef<C> + AsMut<C>> {
        Ok(self
            .timeout_get(&Timeouts::wait_millis(timeout.as_millis() as u64))
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?)
    }

    async fn detach(&self) -> Result<C>
    where
        Self: Sized,
    {
        let v = self
            .get()
            .await
            .map_err(|e| Error::msg(format!("{e:#?}")))?;
        Ok(Object::<DBConnectionManager<C>>::take(v))
    }

    fn resize(&self, max_size: usize) -> Result<()> {
        Ok(self.resize(max_size))
    }
}
