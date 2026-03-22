use mongodb::{Client, bson::doc};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::{
    env, future, net::IpAddr, path::PathBuf, process::Command, str::FromStr, time::Duration,
};
use tank_core::future::{BoxFuture, FutureExt};
use testcontainers_modules::testcontainers::{
    ContainerAsync, Image, ImageExt, TestcontainersError,
    core::{
        CmdWaitFor, ContainerState, ExecCommand, WaitFor,
        logs::{LogFrame, consumer::LogConsumer},
    },
    runners::AsyncRunner,
};
use tokio::fs;

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
                match record {
                    LogFrame::StdOut(..) => log::error!("{log}"),
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

    let mut container = Mongo::repl_set()
        .with_startup_timeout(Duration::from_secs(90)) // give a bit more breathing room
        .with_log_consumer(TestcontainersLogConsumer);

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to("/etc/ca.pem", path.join("tests/assets/ca.pem"))
            .with_copy_to("/etc/mongodb.pem", path.join("tests/assets/mongodb.pem"))
            .with_cmd(vec![
                "--replSet".to_string(),
                "rs".to_string(),
                "--tlsMode".to_string(),
                "preferTLS".to_string(),
                "--tlsCertificateKeyFile".to_string(),
                "/etc/mongodb.pem".to_string(),
                "--tlsCAFile".to_string(),
                "/etc/ca.pem".to_string(),
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

    // Setup connection - no auth yet, plain for init
    let setup_url = format!("mongodb://127.0.0.1:{port}/admin?directConnection=true");
    let client = Client::with_uri_str(&setup_url)
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

    // Final connection string for tests
    let final_url = format!(
        "mongodb://tank-user:armored@127.0.0.1:{port}/military?directConnection=true&authSource=admin{}",
        if ssl {
            format!(
                "&tls=true&tlsCAFile={}&tlsCertificateKeyFile={}",
                path.join("tests/assets/ca.pem").to_str().unwrap(),
                path.join("tests/assets/client.pem").to_str().unwrap(),
            )
        } else {
            "".to_string()
        }
    );

    (final_url, Some(container))
}

async fn generate_ssl_files() -> tank::Result<()> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut ca_params = CertificateParams::new(vec!["root".to_string()])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.use_authority_key_identifier_extension = true;
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let _ = fs::create_dir_all("tests/assets").await;
    fs::write(path.join("tests/assets/ca.pem"), ca_cert.pem()).await?;

    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate()?;
    let mut server_params = CertificateParams::new(["localhost".to_string()])?;
    server_params.use_authority_key_identifier_extension = true;
    server_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::from_str("127.0.0.1").unwrap()),
    ];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "127.0.0.1");
    let server_cert = server_params.signed_by(&server_key, &issuer)?;
    let mongodb_pem = format!("{}\n{}", server_cert.pem(), server_key.serialize_pem());
    fs::write(path.join("tests/assets/mongodb.pem"), mongodb_pem).await?;

    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::new([])?;
    client_params
        .distinguished_name
        .push(DnType::CommonName, "tank-user");
    client_params.is_ca = IsCa::NoCa;
    client_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_cert = client_params.signed_by(&client_key, &issuer)?;
    let client_pem = format!("{}\n{}", client_cert.pem(), client_key.serialize_pem());
    fs::write(path.join("tests/assets/client.pem"), client_pem).await?;

    Ok(())
}
