use std::{borrow::Cow, env, path::PathBuf, process::Command, time::Duration};
use tank_core::Connection;
use tank_tests::{kv_storage, limits, simple};
use testcontainers_modules::{
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    valkey::Valkey,
};

pub(crate) async fn execute_tests<C: Connection>(mut connection: C) {
    simple(&mut connection).await;
    limits(&mut connection).await;
    kv_storage(&mut connection).await;
}

pub async fn init(ssl: bool) -> (String, Option<ContainerAsync<Valkey>>) {
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
    let container = Valkey::default()
        .with_cmd([
            "--user",
            "valkey-commander",
            "on",
            ">supreme",
            "~*",
            "+@all",
        ])
        .with_startup_timeout(Duration::from_secs(60));
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
            "valkey://valkey-commander:supreme@127.0.0.1:{port}/0{}",
            if ssl {
                Cow::Owned(format!(
                    "&sslmode=require&sslrootcert={}&sslcert={}&sslkey={}",
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
