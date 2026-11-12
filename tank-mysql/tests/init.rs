// Shared by the `mysql` and `mariadb` test binaries, so each sees the other's
// helpers as dead code.
#![allow(dead_code)]
use std::{env, future, path::PathBuf, process::Command, time::Duration};
use tank_core::future::{BoxFuture, FutureExt};
use tank_tests::generate_ssl_files;
use testcontainers_modules::{
    mariadb::Mariadb,
    mysql::Mysql,
    testcontainers::{
        ContainerAsync, Image, ImageExt,
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
                LogFrame::StdOut(..) => log::trace!("{log}"),
                LogFrame::StdErr(..) => log::debug!("{log}"),
            }
        })
        .boxed()
    }
}

fn init_sql(db: &str, user: &str, password: &str, ssl: bool) -> Vec<u8> {
    format!(
        r#"
            CREATE DATABASE {db};
            CREATE USER '{user}'@'%' {};
            GRANT ALL PRIVILEGES ON *.* TO '{user}'@'%';
            DROP USER IF EXISTS 'root'@'localhost';
            DROP USER IF EXISTS 'root'@'127.0.0.1';
            DROP USER IF EXISTS 'root'@'::1';
            FLUSH PRIVILEGES;
        "#,
        if ssl {
            "REQUIRE X509".to_string()
        } else {
            format!("IDENTIFIED BY '{password}'")
        }
    )
    .into_bytes()
}

async fn start_db<I>(
    image: I,
    env_var: &str,
    db: &str,
    user: &str,
    password: &str,
    ssl: bool,
) -> (String, Option<ContainerAsync<I>>)
where
    I: Image + Default + Send + 'static,
{
    if let Ok(url) = env::var(env_var) {
        return (url, None);
    }
    if !Command::new("docker")
        .arg("ps")
        .output()
        .map(|o| o.status.success())
        .unwrap_or_default()
    {
        log::error!("Cannot access docker");
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets");
    let mut container = image
        .with_startup_timeout(Duration::from_secs(60))
        .with_log_consumer(TestcontainersLogConsumer);
    if ssl {
        generate_ssl_files(&path).expect("Could not create the certificate files for ssl session");
        container = container
            .with_copy_to("/etc/mysql/conf.d/my.cnf", path.join("my.cnf"))
            .with_copy_to("/etc/mysql/ca.pem", path.join("ca.pem"))
            .with_copy_to("/etc/mysql/server-cert.pem", path.join("server-cert.pem"))
            .with_copy_to("/etc/mysql/server-key.pem", path.join("server-key.pem"));
    }
    let container = container
        .start()
        .await
        .expect("Could not start the container");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("Cannot get the port of MySQL/MariaDB");
    let url = if ssl {
        format!(
            "mysql://{user}@localhost:{port}/{db}?require_ssl=true&ssl_ca={}&ssl_cert={}&ssl_key={}",
            path.join("ca.pem").to_str().unwrap(),
            path.join("client-cert.pem").to_str().unwrap(),
            path.join("client-key.pem").to_str().unwrap(),
        )
    } else {
        format!("mysql://{user}:{password}@localhost:{port}/{db}")
    };
    (url, Some(container))
}

pub async fn init_mysql(ssl: bool) -> (String, Option<ContainerAsync<Mysql>>) {
    let sql = init_sql("mysql_database", "tank-mysql-user", "Sup3r$ecur3", ssl);
    start_db(
        Mysql::default().with_init_sql(sql),
        "TANK_MYSQL_TEST",
        "mysql_database",
        "tank-mysql-user",
        "Sup3r$ecur3",
        ssl,
    )
    .await
}

pub async fn init_mariadb(ssl: bool) -> (String, Option<ContainerAsync<Mariadb>>) {
    let sql = init_sql(
        "mariadb_database",
        "tank-mariadb-user",
        "Th3M0$tS3cu4e",
        ssl,
    );
    start_db(
        Mariadb::default().with_init_sql(sql),
        "TANK_MARIADB_TEST",
        "mariadb_database",
        "tank-mariadb-user",
        "Th3M0$tS3cu4e",
        ssl,
    )
    .await
}
