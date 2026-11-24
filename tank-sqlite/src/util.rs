use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    future::{self, Future},
    hint, ptr,
    task::Poll,
};
use tank_core::{Error, error_message_from_ptr};

/// Owns a raw `sqlite3` connection handle opened with `SQLITE_OPEN_NOMUTEX`.
///
/// Access is serialized by the `Mutex` shared between `SQLiteConnection` and `SQLitePrepared`.
#[derive(Debug)]
pub(crate) struct RawDb(*mut sqlite3);

impl RawDb {
    #[inline(always)]
    pub(crate) const fn null() -> Self {
        Self(ptr::null_mut())
    }

    #[inline(always)]
    pub(crate) const fn as_ptr(&self) -> *mut sqlite3 {
        self.0
    }

    #[inline(always)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut *mut sqlite3 {
        &mut self.0
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn last_error(&self) -> Error {
        unsafe {
            anyhow!(
                "Error [{}] {}",
                sqlite3_errcode(self.0),
                error_message_from_ptr(&sqlite3_errmsg(self.0)),
            )
        }
    }

    #[inline(always)]
    pub(crate) fn finalize(&self, statement: *mut sqlite3_stmt) {
        if !statement.is_null() {
            unsafe {
                if sqlite3_finalize(statement) != SQLITE_OK {
                    hint::cold_path();
                    log::error!(
                        "{:#}",
                        self.last_error()
                            .context("While finalizing a prepared statement")
                    );
                }
            }
        }
    }
}

// SQLite allows moving a NOMUTEX connection across threads when access is serialized.
unsafe impl Send for RawDb {}

impl Drop for RawDb {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                if sqlite3_close(self.0) != SQLITE_OK {
                    hint::cold_path();
                    let error = self
                        .last_error()
                        .context("While closing the sqlite connection");
                    log::error!("{error:#}");
                }
            }
        }
    }
}

/// Yields once to the async executor so synchronous batches stay cooperative.
pub(crate) fn yield_now() -> impl Future<Output = ()> + Send {
    let mut yielded = false;
    future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
}

macro_rules! ensure_rc {
    ($value:expr, $error:expr $(, $context:expr)? $(,)?) => {{
        let value = $value;
        if value != ::libsqlite3_sys::SQLITE_OK {
            ::std::hint::cold_path();
            return Err($error)
                $(.context($context))?
                .inspect_err(|e| ::log::error!("{e:#}"));
        }
        value
    }};
}
pub(crate) use ensure_rc;
