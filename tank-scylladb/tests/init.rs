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

pub async fn init_scylladb(ssl: bool) -> (String, Option<ContainerAsync<ScyllaDB>>) {
    if let Ok(url) = env::var("TANK_SCYLLA_TEST") {
        return (url, None);
    };
    if !Command::new("docker")
        .arg("ps")
        .output()
        .map(|o| o.status.success())
        .unwrap_or_default()
    {
        log::error!("Cannot access docker");
    }
    let mut container = ScyllaDB::default()
        .with_startup_timeout(Duration::from_secs(60))
        .with_log_consumer(TestcontainersLogConsumer);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if ssl {
        generate_ssl_files()
            .await
            .expect("Could not create the certificate files for ssl session");

        // Mount certs into container (override auto-generated ones)
        container = container
            .with_copy_to("/etc/mysql/conf.d/my.cnf", path.join("tests/assets/my.cnf"))
            .with_copy_to(
                "/docker-entrypoint-initdb.d/ca.pem",
                path.join("tests/assets/ca.pem"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/server-cert.pem",
                path.join("tests/assets/server-cert.pem"),
            )
            .with_copy_to(
                "/docker-entrypoint-initdb.d/server-key.pem",
                path.join("tests/assets/server-key.pem"),
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
        .get_host_port_ipv4(9042)
        .await
        .expect("Cannot get the port of ScyllaDB");

    let url = if ssl {
        format!("scylladb://tank-scylladb-user@localhost:{port}/scylla_keyspace",)
    } else {
        format!("scylladb://tank-scylladb-user@localhost:{port}/scylla_keyspace",)
    };
    ScyllaDBDriver::new()
        .connect(url.to_string().replace("/scylla_keyspace", "").into())
        .await
        .expect("Could not connect to the newly create ScyllaDB instance")
        .execute(indoc! {r#"
            CREATE KEYSPACE scylla_keyspace
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        "#})
        .await
        .expect("Could not create the keyspace");
    (url, Some(container))
}

// pub async fn init_mariadb(ssl: bool) -> (String, Option<ContainerAsync<Mariadb>>) {
//     if let Ok(url) = env::var("TANK_MARIADB_TEST") {
//         return (url, None);
//     };
//     if !Command::new("docker")
//         .arg("ps")
//         .output()
//         .map(|o| o.status.success())
//         .unwrap_or_default()
//     {
//         log::error!("Cannot access docker");
//     }
//     let mut container = Mariadb::default()
//         .with_init_sql(
//             format!(
//                 r#"
//                     CREATE DATABASE mariadb_database;
//                     CREATE USER 'tank-mariadb-user'@'%' {};
//                     GRANT ALL PRIVILEGES ON *.* TO 'tank-mariadb-user'@'%';
//                     DROP USER IF EXISTS 'root'@'localhost';
//                     DROP USER IF EXISTS 'root'@'127.0.0.1';
//                     DROP USER IF EXISTS 'root'@'::1';
//                     FLUSH PRIVILEGES;
//                 "#,
//                 if ssl {
//                     "REQUIRE X509"
//                 } else {
//                     "IDENTIFIED BY 'Th3M0$tS3cu4e'"
//                 }
//             )
//             .into_bytes(),
//         )
//         .with_startup_timeout(Duration::from_secs(60))
//         .with_log_consumer(TestcontainersLogConsumer);
//     let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
//     if ssl {
//         generate_ssl_files()
//             .await
//             .expect("Could not create the certificate files for ssl session");

//         // Mount certs into container (override auto-generated ones)
//         container = container
//             .with_copy_to("/etc/mysql/conf.d/my.cnf", path.join("tests/assets/my.cnf"))
//             .with_copy_to("//var/lib/mysql/ca.pem", path.join("tests/assets/ca.pem"))
//             .with_copy_to(
//                 "/var/lib/mysql/server-cert.pem",
//                 path.join("tests/assets/server-cert.pem"),
//             )
//             .with_copy_to(
//                 "/var/lib/mysql/server-key.pem",
//                 path.join("tests/assets/server-key.pem"),
//             );
//     }
//     let container = container
//         .start()
//         .await
//         .expect("Could not start the container");
//     let port = container
//         .get_host_port_ipv4(3306)
//         .await
//         .expect("Cannot get the port of Mysql");

//     (
//         if ssl {
//             format!(
//                 "mysql://tank-mariadb-user@localhost:{port}/mariadb_database?require_ssl=true&ssl_ca={}&ssl_cert={}&ssl_pass={}",
//                 path.join("tests/assets/ca.pem").to_str().unwrap(),
//                 path.join("tests/assets/client.p12").to_str().unwrap(),
//                 urlencoding::encode("my&pass?is=P@$$"),
//             )
//         } else {
//             format!("mysql://tank-mariadb-user:Th3M0$tS3cu4e@localhost:{port}/mariadb_database")
//         },
//         Some(container),
//     )
// }

async fn generate_ssl_files() -> Result<()> {
    //     let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    //     let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()])?;
    //     ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    //     ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    //     ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    //     ca_params.use_authority_key_identifier_extension = true;
    //     let ca_key = KeyPair::generate()?;
    //     let ca_cert = ca_params.self_signed(&ca_key)?;
    //     fs::write(path.join("tests/assets/ca.pem"), ca_cert.pem()).await?;

    //     let ca_issuer = Issuer::from_params(&ca_params, ca_key);

    //     let server_key = KeyPair::generate()?;
    //     let mut server_params = CertificateParams::new(vec!["localhost".to_string()])?;
    //     server_params.use_authority_key_identifier_extension = true;
    //     server_params
    //         .key_usages
    //         .push(KeyUsagePurpose::DigitalSignature);
    //     server_params
    //         .key_usages
    //         .push(KeyUsagePurpose::KeyEncipherment);
    //     server_params
    //         .extended_key_usages
    //         .push(ExtendedKeyUsagePurpose::ServerAuth);
    //     server_params.subject_alt_names = vec![
    //         SanType::DnsName("localhost".try_into()?),
    //         SanType::IpAddress(IpAddr::V4(Ipv4Addr::from_str("127.0.0.1")?)),
    //     ];
    //     server_params
    //         .distinguished_name
    //         .push(DnType::CommonName, "localhost");
    //     let server_cert = server_params.signed_by(&server_key, &ca_issuer)?;
    //     fs::write(path.join("tests/assets/server-cert.pem"), server_cert.pem()).await?;
    //     fs::write(
    //         path.join("tests/assets/server-key.pem"),
    //         server_key.serialize_pem(),
    //     )
    //     .await?;

    //     let client_key = KeyPair::generate()?;
    //     let mut client_params = CertificateParams::new(vec!["tank-mysql-user".to_string()])?;
    //     client_params.is_ca = IsCa::NoCa;
    //     client_params
    //         .key_usages
    //         .push(KeyUsagePurpose::DigitalSignature);
    //     client_params
    //         .extended_key_usages
    //         .push(ExtendedKeyUsagePurpose::ClientAuth);
    //     client_params
    //         .distinguished_name
    //         .push(DnType::CommonName, "tank-mysql-user");
    //     let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;
    //     fs::write(path.join("tests/assets/client-cert.pem"), client_cert.pem()).await?;
    //     fs::write(
    //         path.join("tests/assets/client-key.pem"),
    //         client_key.serialize_pem(),
    //     )
    //     .await?;

    //     let client_p12_path = path.join("tests/assets/client.p12");
    //     if client_p12_path.exists() {
    //         fs::remove_file(&client_p12_path).await.ok();
    //     }

    //     let openssl_output = Command::new("openssl")
    //         .args([
    //             "pkcs12",
    //             "-export",
    //             "-in",
    //             "tests/assets/client-cert.pem",
    //             "-inkey",
    //             "tests/assets/client-key.pem",
    //             "-passout",
    //             "pass:my&pass?is=P@$$",
    //             "-out",
    //             &client_p12_path.to_string_lossy(),
    //         ])
    //         .current_dir(&path)
    //         .output()
    //         .expect("Failed to run openssl");

    //     if !openssl_output.status.success() {
    //         let stderr = String::from_utf8_lossy(&openssl_output.stderr);
    //         log::error!("OpenSSL failed to create PKCS#12: {stderr}");
    //     }

    Ok(())
}
