use mongodb::{Client, bson::doc};
use std::{borrow::Cow, env, future, path::PathBuf, process::Command, time::Duration};
use tank_core::future::{BoxFuture, FutureExt};
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
                            "[{}]{}",
                            json.get("ctx").unwrap_or_default(),
                            json.get("msg").unwrap_or_default()
                        )
                    }
                    LogFrame::StdErr(..) => log::debug!(
                        "[{}]{}",
                        json.get("ctx").unwrap_or_default(),
                        json.get("msg").unwrap_or_default()
                    ),
                }
            } else {
                match record {
                    LogFrame::StdOut(..) => log::error!("{log}",),
                    LogFrame::StdErr(..) => log::error!("{log}"),
                }
            }
        })
        .boxed()
    }
}

const NAME: &str = "mongo";
const TAG: &str = "8.2.4";

#[derive(Default, Debug, Clone)]
enum InstanceKind {
    #[default]
    Standalone,
    ReplSet,
}

#[derive(Default, Debug, Clone)]
pub struct Mongo {
    kind: InstanceKind,
}

impl Mongo {
    pub fn new() -> Self {
        Self {
            kind: InstanceKind::Standalone,
        }
    }

    pub fn repl_set() -> Self {
        Self {
            kind: InstanceKind::ReplSet,
        }
    }
}

impl Image for Mongo {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("mongod startup complete")]
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        match self.kind {
            InstanceKind::Standalone => Vec::<String>::new(),
            InstanceKind::ReplSet => vec!["--replSet".to_string(), "rs".to_string()],
        }
    }

    fn exec_after_start(&self, _: ContainerState) -> Result<Vec<ExecCommand>, TestcontainersError> {
        match self.kind {
            InstanceKind::Standalone => Ok(Default::default()),
            InstanceKind::ReplSet => Ok(vec![
                ExecCommand::new(vec![
                    "mongosh".to_string(),
                    "--quiet".to_string(),
                    "--eval".to_string(),
                    "rs.initiate()".to_string(),
                ])
                .with_cmd_ready_condition(CmdWaitFor::message_on_stdout(
                    "Using a default configuration for the set",
                ))
                .with_container_ready_conditions(vec![
                    WaitFor::message_on_stdout("Transition to primary complete"),
                ]),
            ]),
        }
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
    let container = Mongo::repl_set()
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
    let client = Client::with_uri_str(format!("mongodb://127.0.0.1:{port}?directConnection=true"))
        .await
        .expect("Could not connect to MongoDB for setup");
    client
        .database("admin")
        .run_command(doc! {
            "createUser": "tank-user",
            "pwd": "armored",
            "roles": [ { "role": "root", "db": "admin" } ]
        })
        .await
        .expect("Could not create the user");
    (
        format!(
            "mongodb://tank-user:armored@127.0.0.1:{port}/military?directConnection=true{}",
            if ssl {
                Cow::Owned(format!(
                    "&sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
                    path.join("tests/assets/root.crt").to_str().unwrap(),
                    path.join("tests/assets/client.crt").to_str().unwrap(),
                    path.join("tests/assets/client.key").to_str().unwrap(),
                ))
            } else {
                Cow::Borrowed("&authSource=admin")
            }
        ),
        Some(container),
    )
}
