// Shared by the `valkey` and `redis` test binaries, so each sees the other's
// helpers as dead code.
#![allow(dead_code)]
use std::{borrow::Cow, env, path::PathBuf, process::Command, time::Duration};
use tank_core::{ConnectionPool, Driver, Result};
use tank_tests::{custom, generate_ssl_files, kv_storage, limits, simple};
use testcontainers_modules::{
    testcontainers::{
        ContainerAsync, GenericImage, Image, ImageExt,
        core::{ContainerPort, WaitFor},
        runners::AsyncRunner,
    },
    valkey::Valkey,
};

pub async fn execute_tests<D: Driver>(pool: &mut impl ConnectionPool<D>) -> Result<()> {
    let mut connection = pool
        .get()
        .await
        .expect("Could not get a connection from the pool");
    let connection = connection.as_mut();
    simple(connection).await?;
    limits(connection).await?;
    kv_storage(connection).await?;
    custom(connection).await?;
    Ok(())
}

async fn start_kv<I: Image + Send + 'static>(
    image: I,
    env_var: &str,
    scheme: &str,
    user: &str,
    ssl: bool,
) -> (String, Option<ContainerAsync<I>>) {
    if let Ok(url) = env::var(env_var) {
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
    let mut cmd = vec!["--user", user, "on", ">supreme", "~*", "+@all"];
    let mut container = image.with_startup_timeout(Duration::from_secs(60));
    if ssl {
        generate_ssl_files(&path).expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to("/tmp/ca.pem", path.join("ca.pem"))
            .with_copy_to("/tmp/server-cert.pem", path.join("server-cert.pem"))
            .with_copy_to("/tmp/server-key.pem", path.join("server-key.pem"));
        cmd.extend([
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
        ]);
    }
    let container = container
        .with_cmd(cmd)
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Cannot get the port of Valkey/Redis");
    let url = format!(
        "{scheme}{}://{user}:supreme@127.0.0.1:{port}/0{}",
        if ssl { "s" } else { "" },
        if ssl {
            Cow::Owned(format!(
                "?sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
                path.join("ca.pem").to_str().unwrap(),
                path.join("client-cert.pem").to_str().unwrap(),
                path.join("client-key.pem").to_str().unwrap(),
            ))
        } else {
            Cow::Borrowed("")
        }
    );
    (url, Some(container))
}

pub async fn init_valkey(ssl: bool) -> (String, Option<ContainerAsync<Valkey>>) {
    start_kv(
        Valkey::default(),
        "TANK_VALKEY_TEST",
        "valkey",
        "valkey-commander",
        ssl,
    )
    .await
}

pub async fn init_redis(ssl: bool) -> (String, Option<ContainerAsync<GenericImage>>) {
    let image = GenericImage::new("redis", "7.4.2")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_exposed_port(ContainerPort::Tcp(6379));
    start_kv(image, "TANK_REDIS_TEST", "redis", "redis-commander", ssl).await
}
