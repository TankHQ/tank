#![allow(dead_code)]
use std::{env, future, process::Command, time::Duration};
use tank_core::future::{BoxFuture, FutureExt};
use testcontainers_modules::{
    mssql_server::MssqlServer,
    testcontainers::{
        ContainerAsync, ImageExt,
        core::logs::{LogFrame, consumer::LogConsumer},
        runners::AsyncRunner,
    },
};

struct TestcontainersLogConsumer;

impl LogConsumer for TestcontainersLogConsumer {
    fn accept<'a>(&'a self, record: &'a LogFrame) -> BoxFuture<'a, ()> {
        let log = str::from_utf8(record.bytes())
            .unwrap_or("Invalid error message")
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

pub async fn init_mssql() -> (String, Option<ContainerAsync<MssqlServer>>) {
    if let Ok(url) = env::var("TANK_MSSQL_TEST") {
        return (url, None);
    }
    if !Command::new("docker")
        .arg("ps")
        .output()
        .map(|o| o.status.success())
        .unwrap_or_default()
    {
        log::error!("Cannot access docker");
    }
    let container = MssqlServer::default()
        .with_accept_eula()
        .with_tag("2025-latest")
        .with_startup_timeout(Duration::from_secs(120))
        .with_log_consumer(TestcontainersLogConsumer)
        .start()
        .await
        .expect("Could not start the MSSQL container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("Cannot get the port of MSSQL");
    let password = MssqlServer::DEFAULT_SA_PASSWORD;
    (
        format!("mssql://sa:{password}@localhost:{port}/master?TrustServerCertificate=true"),
        Some(container),
    )
}
