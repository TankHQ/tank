use mysql_async::{InfileData, prelude::GlobalHandler};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tank_core::future::{BoxFuture, FutureExt};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

pub type Reader = Box<dyn AsyncRead + Send + Unpin + 'static>;

#[derive(Default, Clone)]
pub struct Registry {
    streams: Arc<Mutex<HashMap<String, Reader>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&self, name: String, stream: Reader) {
        self.streams.lock().unwrap().insert(name, stream);
    }
}

#[derive(Clone)]
pub struct TankGlobalHandler {
    pub(crate) registry: Registry,
}

impl GlobalHandler for TankGlobalHandler {
    fn handle(
        &self,
        file_name: &[u8],
    ) -> BoxFuture<'static, Result<InfileData, mysql_async::LocalInfileError>> {
        let name = String::from_utf8_lossy(file_name).to_string();
        let registry = self.registry.clone();
        async move {
            let mut guard = registry.streams.lock().unwrap();
            match guard.remove(&name) {
                Some(stream) => {
                    let stream = ReaderStream::new(stream);
                    Ok(Box::pin(stream) as InfileData)
                }
                None => Err(mysql_async::LocalInfileError::other(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Stream not found: {}", name),
                ))),
            }
        }
        .boxed()
    }
}
