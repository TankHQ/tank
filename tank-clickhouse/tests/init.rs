use std::{env, future, time::Duration};
use tank_clickhouse::{ClickHouseConnection, ClickHouseDriver};
use tank_core::{
    Connection,
    future::{BoxFuture, FutureExt},
};
use testcontainers_modules::{
    clickhouse::ClickHouse as ClickHouseImage,
    testcontainers::{
        ContainerAsync, ImageExt,
        core::logs::{LogFrame, consumer::LogConsumer},
        runners::AsyncRunner,
    },
};

pub struct TestcontainersLogConsumer;
impl LogConsumer for TestcontainersLogConsumer {
    fn accept<'a>(&'a self, record: &'a LogFrame) -> BoxFuture<'a, ()> {
        let log = str::from_utf8(record.bytes())
            .unwrap_or("Invalid log message")
            .trim();
        future::ready(if !log.is_empty() {
            match record {
                LogFrame::StdOut(..) => log::trace!("{log}"),
                LogFrame::StdErr(..) => log::debug!("{log}"),
            }
        })
        .boxed()
    }
}

pub async fn init_clickhouse() -> (String, Option<ContainerAsync<ClickHouseImage>>) {
    if let Ok(url) = env::var("TANK_CLICKHOUSE_TEST") {
        return (url, None);
    }

    let container = ClickHouseImage::default()
        .with_startup_timeout(Duration::from_secs(60))
        .with_log_consumer(TestcontainersLogConsumer)
        .start()
        .await
        .expect("Could not start the ClickHouse container");

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Cannot get the native TCP port of ClickHouse");

    let url = format!("clickhouse://default@localhost:{port}/default");

    ClickHouseConnection::connect(&ClickHouseDriver::new(), url.clone().into())
        .await
        .expect("Could not connect to ClickHouse for setup");

    (url, Some(container))
}
