use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use std::{
    borrow::Cow,
    env,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    process::Command,
    str::FromStr,
    time::Duration,
};
use tank_core::Connection;
use tank_tests::{kv_storage, limits, simple};
use testcontainers_modules::{
    testcontainers::{
        ContainerAsync, GenericImage, ImageExt,
        core::{ContainerPort, ContainerRequest, WaitFor},
        runners::AsyncRunner,
    },
    valkey::Valkey,
};
use tokio::fs;

pub(crate) async fn execute_tests(connection: &mut impl Connection) {
    simple(connection).await;
    limits(connection).await;
    kv_storage(connection).await;
}

async fn generate_ssl_files() -> std::io::Result<()> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.use_authority_key_identifier_extension = true;
    let ca_key =
        KeyPair::generate().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let _ = fs::create_dir_all("tests/assets").await;
    fs::write(path.join("tests/assets/ca.pem"), ca_cert.pem()).await?;
    let ca_issuer = Issuer::from_params(&ca_params, ca_key);
    let server_key =
        KeyPair::generate().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
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
        SanType::DnsName("localhost".try_into().unwrap()),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::from_str("127.0.0.1").unwrap())),
    ];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_cert = server_params
        .signed_by(&server_key, &ca_issuer)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(path.join("tests/assets/server-cert.pem"), server_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/server-key.pem"),
        server_key.serialize_pem(),
    )
    .await?;

    let client_key =
        KeyPair::generate().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let mut client_params = CertificateParams::new(vec!["tank-client".to_string()])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    client_params.is_ca = IsCa::NoCa;
    client_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    client_params
        .distinguished_name
        .push(DnType::CommonName, "tank-client");
    let client_cert = client_params
        .signed_by(&client_key, &ca_issuer)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(path.join("tests/assets/client-cert.pem"), client_cert.pem()).await?;
    fs::write(
        path.join("tests/assets/client-key.pem"),
        client_key.serialize_pem(),
    )
    .await?;
    Ok(())
}

pub async fn init_valkey(ssl: bool) -> (String, Option<ContainerAsync<Valkey>>) {
    if let Ok(url) = env::var("TANK_VALKEY_TEST") {
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
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut container: ContainerRequest<Valkey> = Valkey::default().into();
    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to("/tmp/ca.pem", path.join("tests/assets/ca.pem"))
            .with_copy_to(
                "/tmp/server-cert.pem",
                path.join("tests/assets/server-cert.pem"),
            )
            .with_copy_to(
                "/tmp/server-key.pem",
                path.join("tests/assets/server-key.pem"),
            );
    }
    let container = container
        .with_cmd(if ssl {
            vec![
                "--user",
                "valkey-commander",
                "on",
                ">supreme",
                "~*",
                "+@all",
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tmp/server-cert.pem",
                "--tls-key-file",
                "/tmp/server-key.pem",
                "--tls-ca-cert-file",
                "/tmp/ca.pem",
                "--tls-auth-clients",
                "no",
            ]
        } else {
            vec![
                "--user",
                "valkey-commander",
                "on",
                ">supreme",
                "~*",
                "+@all",
            ]
        })
        .with_startup_timeout(Duration::from_secs(60));
    let container = container
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Cannot get the port of Valkey");
    (
        format!(
            "{}://valkey-commander:supreme@127.0.0.1:{port}/0{}",
            if ssl { "valkeys" } else { "valkey" },
            if ssl {
                Cow::Owned(format!(
                    "?sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
                    path.join("tests/assets/ca.pem").to_str().unwrap(),
                    path.join("tests/assets/client-cert.pem").to_str().unwrap(),
                    path.join("tests/assets/client-key.pem").to_str().unwrap(),
                ))
            } else {
                Cow::Borrowed("")
            }
        ),
        Some(container),
    )
}

pub async fn init_redis(ssl: bool) -> (String, Option<ContainerAsync<GenericImage>>) {
    if let Ok(url) = env::var("TANK_REDIS_TEST") {
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
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for ssl session");
    }
    let mut container: ContainerRequest<GenericImage> = GenericImage::new("redis", "7.4.2")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_exposed_port(ContainerPort::Tcp(6379))
        .into();
    if ssl {
        container = container
            .with_copy_to("/tmp/ca.pem", path.join("tests/assets/ca.pem"))
            .with_copy_to(
                "/tmp/server-cert.pem",
                path.join("tests/assets/server-cert.pem"),
            )
            .with_copy_to(
                "/tmp/server-key.pem",
                path.join("tests/assets/server-key.pem"),
            );
    }
    let container = container
        .with_cmd(if ssl {
            vec![
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tmp/server-cert.pem",
                "--tls-key-file",
                "/tmp/server-key.pem",
                "--tls-ca-cert-file",
                "/tmp/ca.pem",
                "--tls-auth-clients",
                "no",
                "--user",
                "redis-commander",
                "on",
                ">supreme",
                "~*",
                "+@all",
            ]
        } else {
            vec!["--user", "redis-commander", "on", ">supreme", "~*", "+@all"]
        })
        .with_startup_timeout(Duration::from_secs(60));
    let container = container
        .start()
        .await
        .expect("Could not start Redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Cannot get the port of Redis");
    (
        format!(
            "{}://redis-commander:supreme@127.0.0.1:{port}/0{}",
            if ssl { "rediss" } else { "redis" },
            if ssl {
                Cow::Owned(format!(
                    "?sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
                    path.join("tests/assets/ca.pem").to_str().unwrap(),
                    path.join("tests/assets/client-cert.pem").to_str().unwrap(),
                    path.join("tests/assets/client-key.pem").to_str().unwrap(),
                ))
            } else {
                Cow::Borrowed("")
            }
        ),
        Some(container),
    )
}
