use mongodb::{Client, bson::doc};
use std::{borrow::Cow, env, future, path::PathBuf, process::Command, time::Duration};
use tank_core::future::{BoxFuture, FutureExt};
use tank_tests::generate_ssl_files;
use testcontainers_modules::testcontainers::{
    ContainerAsync, Image, ImageExt, TestcontainersError,
    core::{
        CmdWaitFor, ContainerState, ExecCommand, WaitFor,
        logs::{LogFrame, consumer::LogConsumer},
    },
    runners::AsyncRunner,
};

struct TestcontainersLogConsumer;
impl LogConsumer for TestcontainersLogConsumer {
    fn accept<'a>(&'a self, record: &'a LogFrame) -> BoxFuture<'a, ()> {
        let log = str::from_utf8(record.bytes())
            .unwrap_or("Invalid error message")
            .trim();
        future::ready(if !log.is_empty() {
            if let Ok(serde_json::Value::Object(json)) =
                serde_json::from_str::<serde_json::Value>(log)
            {
                match record {
                    LogFrame::StdOut(..) => {
                        log::trace!(
                            "[{}] {}",
                            json.get("ctx").unwrap_or_default(),
                            json.get("msg").unwrap_or_default()
                        )
                    }
                    LogFrame::StdErr(..) => log::debug!(
                        "[{}] {}",
                        json.get("ctx").unwrap_or_default(),
                        json.get("msg").unwrap_or_default()
                    ),
                }
            } else {
                log::error!("{log}");
            }
        })
        .boxed()
    }
}

#[derive(Default, Debug, Clone)]
pub struct Mongo;

impl Image for Mongo {
    fn name(&self) -> &str {
        "mongo"
    }

    fn tag(&self) -> &str {
        "8.2.4"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("mongod startup complete")]
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        ["--replSet", "rs"]
    }

    fn exec_after_start(&self, _: ContainerState) -> Result<Vec<ExecCommand>, TestcontainersError> {
        Ok(vec![
            ExecCommand::new(["mongosh", "--quiet", "--eval", "rs.initiate()"])
                .with_cmd_ready_condition(CmdWaitFor::message_on_stdout(
                    "Using a default configuration for the set",
                ))
                .with_container_ready_conditions(vec![WaitFor::message_on_stdout(
                    "Transition to primary complete",
                )]),
        ])
    }
}

pub async fn init(ssl: bool) -> (String, Option<ContainerAsync<Mongo>>) {
    if let Ok(url) = env::var("TANK_MONGODB_TEST") {
        return (url, None);
    }
    if !Command::new("docker")
        .arg("ps")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        log::error!("Cannot access docker");
    }

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets");
    let mut container = Mongo
        .with_startup_timeout(Duration::from_secs(90))
        .with_log_consumer(TestcontainersLogConsumer);

    if ssl {
        generate_ssl_files(&path).expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to("/etc/ca.pem", path.join("ca.pem"))
            .with_copy_to("/etc/mongodb.pem", path.join("server.pem"))
            .with_cmd([
                "--replSet",
                "rs",
                "--tlsMode",
                "preferTLS",
                "--tlsCertificateKeyFile",
                "/etc/mongodb.pem",
                "--tlsCAFile",
                "/etc/ca.pem",
            ]);
    }

    let container = container
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("Cannot get the port of MongoDB");

    let setup_url = format!("mongodb://127.0.0.1:{port}/admin?directConnection=true");
    let client = Client::with_uri_str(&setup_url)
        .await
        .expect("Could not connect to MongoDB for setup");
    client
        .database("admin")
        .run_command(doc! {
            "createUser": "tank-user",
            "pwd": "armored",
            "roles": [{ "role": "root", "db": "admin" }]
        })
        .await
        .expect("Could not create the user");

    let final_url = format!(
        "mongodb://tank-user:armored@127.0.0.1:{port}/military?directConnection=true&authSource=admin{}",
        if ssl {
            Cow::Owned(format!(
                "&tls=true&tlsCAFile={}&tlsCertificateKeyFile={}",
                path.join("ca.pem").to_str().unwrap(),
                path.join("client.pem").to_str().unwrap(),
            ))
        } else {
            Cow::Borrowed("")
        }
    );

    (final_url, Some(container))
}
