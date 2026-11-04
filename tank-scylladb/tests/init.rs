// Shared by the `scylladb`, `cassandra` and `connection` test binaries, so
// each sees the others' helpers as dead code.
#![allow(dead_code)]
use indoc::indoc;
use std::{borrow::Cow, env, future, path::PathBuf, time::Duration};
use tank_core::{
    Connection, ConnectionPool, Driver, Executor,
    future::{BoxFuture, FutureExt},
};
use tank_scylladb::{CassandraConnection, CassandraDriver, ScyllaDBConnection, ScyllaDBDriver};
use tank_tests::{
    ambiguity, cheat_sheet, custom, enums, generate_ssl_files, identifiers, interval, kv_storage,
    limits, metrics, service, simple, trade_multiple, trade_simple, transaction1,
};
use testcontainers_modules::{
    scylladb::ScyllaDB,
    testcontainers::{
        ContainerAsync, GenericImage, ImageExt,
        core::{
            ContainerPort, WaitFor,
            logs::{LogFrame, consumer::LogConsumer},
        },
        runners::AsyncRunner,
    },
};
use url::Url;

pub async fn execute_tests<D: Driver>(pool: &mut impl ConnectionPool<D>) {
    let mut connection = pool
        .get()
        .await
        .expect("Could not get a connection from the pool");
    macro_rules! do_test {
        ($test_function:ident $(, $args:expr )* $(,)?) => {
            Box::pin($test_function(&mut connection, $($args),*))
                .await
                .expect("A tank-tests test failed")
        };
    }
    do_test!(simple);
    do_test!(kv_storage);
    do_test!(trade_simple);
    do_test!(trade_multiple);
    do_test!(limits);
    do_test!(interval);
    do_test!(transaction1);
    do_test!(metrics);
    do_test!(ambiguity);
    do_test!(service);
    do_test!(enums);
    do_test!(custom);
    do_test!(identifiers);
    do_test!(cheat_sheet);
}

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

pub async fn init_scylladb(ssl: bool) -> (String, Option<ContainerAsync<ScyllaDB>>) {
    if let Ok(url) = env::var("TANK_SCYLLADB_TEST") {
        return (url, None);
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets");
    let mut image = ScyllaDB::default()
        .with_tag("2026.1.2")
        .with_startup_timeout(Duration::from_secs(120))
        .with_log_consumer(TestcontainersLogConsumer)
        .with_cmd([
            "--broadcast-rpc-address",
            "127.0.0.1",
            "--native-shard-aware-transport-port",
            "0",
        ]);
    if ssl {
        generate_ssl_files(&path).expect("Could not create the certificate files for SSL session");
        image = image
            .with_copy_to("/etc/scylla/scylla.yaml", path.join("scylla.yaml"))
            .with_copy_to("/etc/scylla/ca.pem", path.join("ca.pem"))
            .with_copy_to("/etc/scylla/server-cert.pem", path.join("server-cert.pem"))
            .with_copy_to("/etc/scylla/server-key.pem", path.join("server-key.pem"));
    }
    let container = image
        .start()
        .await
        .expect("Could not start the ScyllaDB container");
    let port = container
        .get_host_port_ipv4(9042)
        .await
        .expect("Cannot get the port (9042) of ScyllaDB");
    let final_url = format!(
        "scylladb://localhost:{port}/scylla_keyspace{}",
        if ssl {
            Cow::Owned(format!(
                "?ssl_ca={}&ssl_cert={}&ssl_key={}",
                path.join("ca.pem").to_string_lossy(),
                path.join("client-cert.pem").to_string_lossy(),
                path.join("client-key.pem").to_string_lossy(),
            ))
        } else {
            Cow::Borrowed("")
        }
    );
    let mut plain_url = Url::parse(&final_url).expect("The URL was not correct");
    plain_url.set_path("");
    ScyllaDBConnection::connect(&ScyllaDBDriver::new(), plain_url.to_string().into())
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

pub async fn init_cassandra(ssl: bool) -> (String, Option<ContainerAsync<GenericImage>>) {
    if let Ok(url) = env::var("TANK_CASSANDRA_TEST") {
        return (url, None);
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets");
    let mut image = GenericImage::new("cassandra", "5.0.8")
        .with_exposed_port(ContainerPort::Tcp(9042))
        .with_ready_conditions(vec![WaitFor::message_on_either_std(
            "Created default superuser role",
        )])
        .with_startup_timeout(Duration::from_secs(120))
        .with_log_consumer(TestcontainersLogConsumer)
        .with_env_var("CASSANDRA_BROADCAST_RPC_ADDRESS", "127.0.0.1");
    if ssl {
        generate_ssl_files(&path).expect("Could not create the certificate files for SSL session");
        image = image
            .with_copy_to("/etc/cassandra/cassandra.yaml", path.join("cassandra.yaml"))
            .with_copy_to("/etc/cassandra/ca.pem", path.join("ca.pem"))
            .with_copy_to("/etc/cassandra/server.pem", path.join("server.pem"));
    }
    let container = image
        .start()
        .await
        .expect("Could not start the Cassandra container");
    let port = container
        .get_host_port_ipv4(9042)
        .await
        .expect("Cannot get the port (9042) of Cassandra");
    let final_url = format!(
        "cassandra://localhost:{port}/cassandra_keyspace{}",
        if ssl {
            Cow::Owned(format!(
                "?ssl_ca={}&ssl_cert={}&ssl_key={}",
                path.join("ca.pem").to_string_lossy(),
                path.join("client-cert.pem").to_string_lossy(),
                path.join("client-key.pem").to_string_lossy(),
            ))
        } else {
            Cow::Borrowed("")
        }
    );
    let mut plain_url = Url::parse(&final_url).expect("The URL was not correct");
    plain_url.set_path("");
    CassandraConnection::connect(&CassandraDriver::new(), plain_url.to_string().into())
        .await
        .expect("Could not connect to Cassandra for setup")
        .execute(indoc! {r#"
            CREATE KEYSPACE IF NOT EXISTS cassandra_keyspace
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        "#})
        .await
        .expect("Could not create the keyspace");
    (final_url, Some(container))
}
