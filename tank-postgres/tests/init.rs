use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::{
    borrow::Cow, env, future, net::IpAddr, path::PathBuf, process::Command, str::FromStr,
    time::Duration,
};
use tank_core::{
    Result,
    future::{BoxFuture, FutureExt},
};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{
        ContainerAsync, ImageExt,
        core::logs::{LogFrame, consumer::LogConsumer},
        runners::AsyncRunner,
    },
};
use tokio::fs;

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

pub async fn init(ssl: bool) -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = env::var("TANK_POSTGRES_TEST") {
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
    let mut container = Postgres::default()
        .with_db_name("military")
        .with_user("tank-user")
        .with_password("armored")
        .with_startup_timeout(Duration::from_secs(60))
        .with_log_consumer(TestcontainersLogConsumer);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to(
                "/docker-entrypoint-initdb.d/pg_hba.conf",
                path.join("tests/assets/pg_hba.conf"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/root.crt",
                path.join("tests/assets/root.crt"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/server.crt",
                path.join("tests/assets/server.crt"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/server.key",
                path.join("tests/assets/server.key"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/00-ssl.sh",
                path.join("tests/assets/00-ssl.sh"),
            );
    }
    let container = container
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Cannot get the port of Postgres");
    (
        format!(
            "postgres://tank-user:armored@127.0.0.1:{port}/military{}",
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

async fn generate_ssl_files() -> Result<()> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut ca_params = CertificateParams::new(vec!["root".to_string()])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.use_authority_key_identifier_extension = true;
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let _ = fs::create_dir_all("tests/assets").await;
    fs::write(path.join("tests/assets/root.crt"), ca_cert.pem()).await?;
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
    fs::write(path.join("tests/assets/server.crt"), server_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/server.key"),
        server_key.serialize_pem(),
    )
    .await?;

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
    fs::write(path.join("tests/assets/client.crt"), client_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/client.key"),
        client_key.serialize_pem(),
    )
    .await?;

    Ok(())
}
