use anyhow::anyhow;
use libsqlite3_sys::*;
use std::{
    fmt::{self, Debug, Formatter},
    future::{self, Future},
    hint, mem, ptr,
    sync::{Arc, Mutex, MutexGuard},
    task::Poll,
};
use tank_core::{Error, error_message_from_ptr};

/// Owns a raw `sqlite3` connection handle opened with `SQLITE_OPEN_NOMUTEX`.
///
/// Access is serialized by the [`Mutex`] in [`SharedDb`].
#[derive(Debug)]
pub(crate) struct RawDb(*mut sqlite3);

// SQLite allows moving a NOMUTEX connection across threads when access is serialized.
unsafe impl Send for RawDb {}

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
}

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

/// Shared, mutex-serialized ownership of a [`RawDb`].
///
/// Prepared statements keep a clone so the database cannot close until every
/// `sqlite3_stmt` has been finalized.
#[derive(Clone, Debug)]
pub(crate) struct SharedDb(Arc<Mutex<RawDb>>);

impl SharedDb {
    pub(crate) fn new(db: RawDb) -> Self {
        Self(Arc::new(Mutex::new(db)))
    }

    #[inline(always)]
    pub(crate) fn lock(&self) -> MutexGuard<'_, RawDb> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline(always)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Temporary `sqlite3_stmt` guard bound to a locked [`RawDb`].
pub(crate) struct ScopedStmt<'a> {
    ptr: *mut sqlite3_stmt,
    db: &'a RawDb,
}

impl<'a> ScopedStmt<'a> {
    #[inline(always)]
    pub(crate) const fn null(db: &'a RawDb) -> Self {
        Self {
            ptr: ptr::null_mut(),
            db,
        }
    }

    #[inline(always)]
    pub(crate) const fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline(always)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut *mut sqlite3_stmt {
        &mut self.ptr
    }

    #[inline(always)]
    pub(crate) fn into_owned(mut self, db: SharedDb) -> OwnedStmt {
        let ptr = mem::replace(&mut self.ptr, ptr::null_mut());
        OwnedStmt { db, ptr }
    }
}

impl Drop for ScopedStmt<'_> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                if sqlite3_finalize(self.ptr) != SQLITE_OK {
                    hint::cold_path();
                    log::error!(
                        "{:#}",
                        self.db
                            .last_error()
                            .context("While finalizing a prepared statement")
                    );
                }
            }
        }
    }
}

/// Owns a `sqlite3_stmt` alongside a shared reference to its parent [`SharedDb`].
pub(crate) struct OwnedStmt {
    db: SharedDb,
    ptr: *mut sqlite3_stmt,
}

// Every SQLite C API call on `ptr` requires locking `db`.
unsafe impl Send for OwnedStmt {}
unsafe impl Sync for OwnedStmt {}

impl OwnedStmt {
    #[inline(always)]
    pub(crate) fn lock(&self) -> MutexGuard<'_, RawDb> {
        self.db.lock()
    }

    #[inline(always)]
    pub(crate) fn belongs_to(&self, db: &SharedDb) -> bool {
        self.db.ptr_eq(db)
    }

    #[inline(always)]
    pub(crate) const fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline(always)]
    pub(crate) const fn as_ptr(&self, _db: &RawDb) -> *mut sqlite3_stmt {
        self.ptr
    }

    /// Runs `finish` and finalizes the statement under a single [`RawDb`] lock.
    pub(crate) fn finish_and_finalize(&mut self, finish: impl FnOnce(&RawDb, *mut sqlite3_stmt)) {
        if !self.ptr.is_null() {
            let db = self.db.lock();
            let stmt = ScopedStmt {
                ptr: mem::replace(&mut self.ptr, ptr::null_mut()),
                db: &db,
            };
            finish(&db, stmt.ptr);
        }
    }
}

impl Debug for OwnedStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedStmt")
            .field("ptr", &self.ptr)
            .finish()
    }
}

impl Drop for OwnedStmt {
    fn drop(&mut self) {
        self.finish_and_finalize(|_, _| {});
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
