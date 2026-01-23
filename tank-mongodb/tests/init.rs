use std::{borrow::Cow, env, future, path::PathBuf, process::Command, time::Duration};
use tank_core::future::{BoxFuture, FutureExt};
use testcontainers_modules::{
    mongo::Mongo,
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
                LogFrame::StdOut(..) => log::trace!("{log}",),
                LogFrame::StdErr(..) => log::debug!("{log}"),
            }
        })
        .boxed()
    }
}

pub async fn init(ssl: bool) -> (String, Option<ContainerAsync<Mongo>>) {
    if let Ok(url) = env::var("TANK_MONGODB_TEST") {
        return (url, None);
    };
    if !Command::new("docker")
        .arg("ps")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        log::error!("Cannot access docker");
    }
    let container = Mongo::default()
        .with_env_var("MONGO_INITDB_ROOT_USERNAME", "tank-user")
        .with_env_var("MONGO_INITDB_ROOT_PASSWORD", "armored")
        .with_startup_timeout(Duration::from_secs(60))
        .with_log_consumer(TestcontainersLogConsumer);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if ssl {}
    let container = container
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("Cannot get the port of Postgres");
    (
        format!(
            "mongodb://tank-user:armored@127.0.0.1:{port}/{}",
            if ssl {
                Cow::Owned(format!(
                    "?sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
                    path.join("tests/assets/root.crt").to_str().unwrap(),
                    path.join("tests/assets/client.crt").to_str().unwrap(),
                    path.join("tests/assets/client.key").to_str().unwrap(),
                ))
            } else {
                Cow::Borrowed("")
            }
        ),
        Some(container),
    )
}
