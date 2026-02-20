mod aggregates;
mod ambiguity;
mod arrays1;
mod arrays2;
mod books;
mod complex;
mod conditions;
mod enums;
mod insane;
mod interval;
mod limits;
mod math;
mod metrics;
mod multiple;
mod operations;
mod orders;
mod other;
mod readme;
mod requests;
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
pub use books::*;
pub use complex::*;
pub use conditions::*;
pub use enums::*;
pub use insane::*;
pub use interval::*;
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
pub use shopping::*;
pub use simple::*;
use std::env;
use tank::Connection;
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

pub async fn execute_tests<C: Connection>(mut connection: C) {
    macro_rules! do_test {
        ($test_function:ident) => {
            Box::pin($test_function(&mut connection)).await
        };
    }
    do_test!(simple);
    do_test!(trade_simple);
    do_test!(trade_multiple);
    do_test!(users);
    do_test!(aggregates);
    do_test!(books);
    do_test!(complex);
    do_test!(insane);
    do_test!(limits);
    #[cfg(not(feature = "disable-multiple-statements"))]
    do_test!(multiple);
    #[cfg(not(feature = "disable-intervals"))]
    do_test!(interval);
    #[cfg(not(feature = "disable-arrays"))]
    do_test!(arrays1);
    #[cfg(not(feature = "disable-arrays"))]
    do_test!(arrays2);
    #[cfg(not(feature = "disable-transactions"))]
    do_test!(transaction1);
    do_test!(transaction2);
    do_test!(shopping);
    do_test!(orders);
    do_test!(times);
    do_test!(conditions);
    do_test!(readme).expect("Readme examples test did not succeed");
    do_test!(operations).expect("Operations examples test did not succeed");
    do_test!(advanced_operations).expect("Advanced operations examples test did not succeed");
    do_test!(metrics);
    do_test!(math);
    do_test!(ambiguity);
    do_test!(other);
    do_test!(enums);
    do_test!(requests);
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
