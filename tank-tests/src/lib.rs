mod aggregates;
mod ambiguity;
mod arrays1;
mod arrays2;
mod binary;
mod bindings;
mod books;
mod case;
mod cheat_sheet;
mod complex;
mod conditions;
mod custom;
mod decimals;
mod enums;
mod identifiers;
mod insane;
mod insert_builder;
mod interval;
mod keywords;
mod kv_storage;
mod limits;
mod math;
mod metrics;
mod multiple;
mod operations;
mod orders;
mod other;
mod prepared;
mod readme;
mod requests;
mod service;
mod shopping;
mod simple;
mod time;
mod trade;
mod transaction1;
mod transaction2;
mod user;

pub use aggregates::*;
pub use ambiguity::*;
pub use arrays1::*;
pub use arrays2::*;
pub use binary::*;
pub use bindings::*;
pub use books::*;
pub use case::*;
pub use cheat_sheet::*;
pub use complex::*;
pub use conditions::*;
pub use custom::*;
pub use decimals::*;
pub use enums::*;
pub use identifiers::*;
pub use insane::*;
pub use insert_builder::*;
pub use interval::*;
pub use keywords::*;
pub use kv_storage::*;
pub use limits::*;
use log::LevelFilter;
pub use math::*;
pub use metrics::*;
pub use multiple::*;
pub use operations::*;
pub use orders::*;
pub use other::*;
pub use prepared::*;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
pub use readme::*;
pub use requests::*;
pub use service::*;
pub use shopping::*;
pub use simple::*;
use std::{
    env, fs,
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::Path,
    pin::Pin,
};
use tank::{ConnectionPool, Driver, PooledConnection, Result};
pub use time::*;
pub use trade::*;
pub use transaction1::*;
pub use transaction2::*;
pub use user::*;

pub fn generate_ssl_files(dir: impl AsRef<Path>) -> Result<()> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;

    let mut ca_params = CertificateParams::new(vec!["Test CA".to_string()])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.use_authority_key_identifier_extension = true;
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    fs::write(dir.join("ca.pem"), ca_cert.pem())?;
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
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)),
    ];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_cert = server_params.signed_by(&server_key, &ca_issuer)?;
    let server_cert_pem = server_cert.pem();
    let server_key_pem = server_key.serialize_pem();
    fs::write(dir.join("server-cert.pem"), &server_cert_pem)?;
    fs::write(dir.join("server-key.pem"), &server_key_pem)?;
    fs::write(
        dir.join("server.pem"),
        format!("{server_cert_pem}\n{server_key_pem}"),
    )?;

    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::new(vec!["tank-client".to_string()])?;
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
    let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;
    let client_cert_pem = client_cert.pem();
    let client_key_pem = client_key.serialize_pem();
    fs::write(dir.join("client-cert.pem"), &client_cert_pem)?;
    fs::write(dir.join("client-key.pem"), &client_key_pem)?;
    fs::write(
        dir.join("client.pem"),
        format!("{client_cert_pem}\n{client_key_pem}"),
    )?;

    Ok(())
}

pub fn init_logs() {
    let mut logger = env_logger::builder();
    logger
        .is_test(true)
        .format_file(true)
        .format_line_number(true);
    if env::var("RUST_LOG").is_err() {
        logger.filter_level(LevelFilter::Warn);
    }
    let _ = logger.try_init();
}

pub async fn execute_tests<D: Driver + 'static>(pool: &mut impl ConnectionPool<D>) {
    type TestFn<D> = Box<
        dyn for<'c> Fn(
            &'c mut PooledConnection<D>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'c>>,
    >;
    fn boxed<D: Driver + 'static, F>(f: F) -> TestFn<D>
    where
        F: Copy + 'static + for<'c> AsyncFn(&'c mut PooledConnection<D>) -> Result<()>,
    {
        Box::new(move |c| Box::pin(async move { f(c).await }))
    }

    let mut connection = pool
        .get()
        .await
        .expect("Could not get a connection from the pool");
    let tests: Vec<TestFn<D>> = vec![
        boxed(simple),
        boxed(kv_storage),
        boxed(trade_simple),
        boxed(trade_multiple),
        boxed(users),
        boxed(aggregates),
        boxed(binary),
        boxed(books),
        boxed(complex),
        boxed(insane),
        boxed(limits),
        #[cfg(not(feature = "disable-multiple-statements"))]
        boxed(multiple),
        #[cfg(not(feature = "disable-intervals"))]
        boxed(interval),
        #[cfg(not(feature = "disable-arrays"))]
        boxed(arrays1),
        #[cfg(not(feature = "disable-arrays"))]
        boxed(arrays2),
        #[cfg(not(feature = "disable-transactions"))]
        boxed(transaction1),
        #[cfg(not(feature = "disable-transactions"))]
        boxed(transaction2),
        boxed(shopping),
        boxed(orders),
        boxed(times),
        boxed(conditions),
        #[cfg(not(feature = "disable-case"))]
        boxed(case),
        boxed(metrics),
        boxed(math),
        boxed(ambiguity),
        boxed(other),
        boxed(service),
        boxed(enums),
        boxed(custom),
        boxed(decimals),
        boxed(prepared),
        boxed(bindings),
        boxed(requests),
        boxed(keywords),
        boxed(identifiers),
        boxed(readme),
        boxed(operations),
        boxed(advanced_operations),
        boxed(cheat_sheet),
        boxed(insert_builder),
    ];
    for test in tests {
        test(&mut connection)
            .await
            .expect("A tank-tests test failed");
    }
}

#[macro_export]
macro_rules! silent_logs {
    ($($code:tt)+) => {{
        let level = log::max_level();
        log::set_max_level(log::LevelFilter::Off);
        $($code)+
        log::set_max_level(level);
    }};
}
