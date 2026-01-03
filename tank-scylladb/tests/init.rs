use rcgen::{
    CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    SanType,
};
use std::{
    env, future,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    process::Command,
    str::FromStr,
    time::Duration,
};
use tank_core::{
    Driver, Executor, Result,
    future::{BoxFuture, FutureExt},
    indoc::indoc,
};
use tank_scylladb::ScyllaDBDriver;
use testcontainers_modules::{
    scylladb::ScyllaDB,
    testcontainers::{
        ContainerAsync, ImageExt,
        core::{
            ContainerPort,
            logs::{LogFrame, consumer::LogConsumer},
        },
        runners::AsyncRunner,
    },
};
use tokio::fs;
use url::Url;

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

pub async fn init_scylladb(ssl: bool) -> (String, Option<ContainerAsync<ScyllaDB>>) {
    if let Ok(url) = env::var("TANK_SCYLLA_TEST") {
        return (url, None);
    };
    let mut image = ScyllaDB::default()
        .with_startup_timeout(Duration::from_secs(120))
        .with_log_consumer(TestcontainersLogConsumer);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for SSL session");
        image = image
            .with_mapped_port(9042, ContainerPort::Tcp(9042))
            .with_mapped_port(9142, ContainerPort::Tcp(9142))
            .with_copy_to(
                "/etc/scylla/scylla.yaml",
                path.join("tests/assets/scylla.yaml"),
            )
            .with_copy_to("/etc/scylla/ca.pem", path.join("tests/assets/ca.pem"))
            .with_copy_to(
                "/etc/scylla/scylla.crt",
                path.join("tests/assets/scylla.crt"),
            )
            .with_copy_to(
                "/etc/scylla/scylla.key",
                path.join("tests/assets/scylla.key"),
            );
    }
    let container = image
        .start()
        .await
        .expect("Could not start the ScyllaDB container");
    let plaintext_port = container
        .get_host_port_ipv4(9042)
        .await
        .expect("Cannot get the plaintext port (9042) of ScyllaDB");
    let final_url = if ssl {
        let ssl_host_port = container
            .get_host_port_ipv4(9142)
            .await
            .expect("Cannot get the SSL port");
        let params = format!(
            "sslca={}&sslcert={}&sslkey={}",
            path.join("tests/assets/ca.pem").to_string_lossy(),
            path.join("tests/assets/client-cert.pem").to_string_lossy(),
            path.join("tests/assets/client-key.pem").to_string_lossy(),
        );
        format!("scylladb://localhost:{ssl_host_port}/scylla_keyspace?{params}")
    } else {
        format!("scylladb://localhost:{plaintext_port}/scylla_keyspace")
    };
    let mut plain_url = Url::parse(&final_url).expect("The URL was not correct");
    plain_url.set_path("");
    ScyllaDBDriver::new()
        .connect(plain_url.to_string().into())
        .await
        .expect("Could not connect to ScyllaDB for setup")
        .execute(indoc! {r#"
            CREATE KEYSPACE IF NOT EXISTS scylla_keyspace
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        "#})
        .await
        .expect("Could not create the keyspace");
    (final_url, Some(container))
}

async fn generate_ssl_files() -> Result<()> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()])?;
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.use_authority_key_identifier_extension = true;
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    fs::write(path.join("tests/assets/ca.pem"), ca_cert.pem()).await?;

    let ca_issuer = Issuer::from_params(&ca_params, ca_key);

    let server_key = KeyPair::generate()?;
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()])?;
    server_params.use_authority_key_identifier_extension = true;
    server_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    server_params
        .key_usages
        .push(KeyUsagePurpose::KeyEncipherment);
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::from_str("127.0.0.1")?)),
    ];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_cert = server_params.signed_by(&server_key, &ca_issuer)?;
    fs::write(path.join("tests/assets/scylla.crt"), server_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/scylla.key"),
        server_key.serialize_pem(),
    )
    .await?;

    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::new(vec!["tank-mysql-user".to_string()])?;
    client_params.is_ca = IsCa::NoCa;
    client_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    client_params
        .distinguished_name
        .push(DnType::CommonName, "tank-mysql-user");
    let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;
    fs::write(path.join("tests/assets/client-cert.pem"), client_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/client-key.pem"),
        client_key.serialize_pem(),
    )
    .await?;

    let client_p12_path = path.join("tests/assets/client.p12");
    if client_p12_path.exists() {
        fs::remove_file(&client_p12_path).await.ok();
    }

    let openssl_output = Command::new("openssl")
        .args([
            "pkcs12",
            "-export",
            "-in",
            "tests/assets/client-cert.pem",
            "-inkey",
            "tests/assets/client-key.pem",
            "-passout",
            "pass:my&pass?is=P@$$",
            "-out",
            &client_p12_path.to_string_lossy(),
        ])
        .current_dir(&path)
        .output()
        .expect("Failed to run openssl");

    if !openssl_output.status.success() {
        let stderr = String::from_utf8_lossy(&openssl_output.stderr);
        log::error!("OpenSSL failed to create PKCS#12: {stderr}");
    }

    Ok(())
}
