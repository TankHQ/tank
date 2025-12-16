//! SQLite driver for `tank`.
mod cbox;
mod connection;
mod driver;
mod extract;
mod prepared;
mod sql_writer;
mod transaction;

use std::{ffi::CStr, ptr};

pub(crate) use cbox::*;
pub use connection::*;
pub use driver::*;
pub use prepared::*;
pub use transaction::*;

pub(crate) fn error_message_from_ptr(ptr: &'_ *const i8) -> &'_ str {
    unsafe {
        if *ptr != ptr::null() {
            CStr::from_ptr(*ptr)
                .to_str()
                .unwrap_or("Unknown error: the error message was not a valid utf8 string")
        } else {
            "Unknown error: could not extract the error message"
        }
    }
}
