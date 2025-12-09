mod aggregates;
mod arrays1;
mod arrays2;
mod books;
mod complex;
mod insane;
mod interval;
mod limits;
mod multiple;
mod operations;
mod orders;
mod readme;
mod shopping;
mod simple;
mod time;
mod trade;
mod transaction1;
mod user;

use crate::{
    books::books,
    complex::complex,
    insane::insane,
    limits::limits,
    operations::{advanced_operations, operations},
    orders::orders,
    shopping::shopping,
    simple::simple,
    time::times,
    trade::{trade_multiple, trade_simple},
    transaction1::transaction1,
    user::users,
};
use aggregates::aggregates;

#[cfg(not(feature = "disable-arrays"))]
use arrays1::arrays1;
#[cfg(not(feature = "disable-arrays"))]
use arrays2::arrays2;
#[cfg(not(feature = "disable-intervals"))]
use interval::interval;
use log::LevelFilter;
#[cfg(not(feature = "disable-multiple-statements"))]
use multiple::multiple;
use readme::readme;
use std::env;
use tank::Connection;

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

pub async fn execute_tests<C: Connection>(mut connection: C) {
    simple(&mut connection).await;
    trade_simple(&mut connection).await;
    trade_multiple(&mut connection).await;
    users(&mut connection).await;
    aggregates(&mut connection).await;
    books(&mut connection).await;
    complex(&mut connection).await;
    insane(&mut connection).await;
    limits(&mut connection).await;
    #[cfg(not(feature = "disable-multiple-statements"))]
    multiple(&mut connection).await;
    #[cfg(not(feature = "disable-transactions"))]
    transaction1(&mut connection).await;
    #[cfg(not(feature = "disable-intervals"))]
    interval(&mut connection).await;
    #[cfg(not(feature = "disable-arrays"))]
    arrays1(&mut connection).await;
    #[cfg(not(feature = "disable-arrays"))]
    arrays2(&mut connection).await;
    shopping(&mut connection).await;
    orders(&mut connection).await;
    times(&mut connection).await;
    readme(&mut connection)
        .await
        .expect("Readme examples test did not succeed");
    operations(&mut connection)
        .await
        .expect("Operations examples test did not succeed");
    advanced_operations(&mut connection)
        .await
        .expect("Advanced operations examples test did not succeed")
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
