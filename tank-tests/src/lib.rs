mod aggregates;
mod ambiguity;
mod arrays1;
mod arrays2;
mod binary;
mod books;
mod cheat_sheet;
mod complex;
mod conditions;
mod custom;
mod enums;
mod identifiers;
mod insane;
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
pub use books::*;
pub use cheat_sheet::*;
pub use complex::*;
pub use conditions::*;
pub use custom::*;
pub use enums::*;
pub use identifiers::*;
pub use insane::*;
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
pub use readme::*;
pub use requests::*;
pub use service::*;
pub use shopping::*;
pub use simple::*;
use std::{env, future::Future, pin::Pin};
use tank::{ConnectionPool, Driver, PooledConnection, Result};
pub use time::*;
pub use trade::*;
pub use transaction1::*;
pub use transaction2::*;
pub use user::*;

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
        boxed(metrics),
        boxed(math),
        boxed(ambiguity),
        boxed(other),
        boxed(service),
        boxed(enums),
        boxed(custom),
        boxed(requests),
        boxed(keywords),
        boxed(identifiers),
        boxed(readme),
        boxed(operations),
        boxed(advanced_operations),
        boxed(cheat_sheet),
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
