use crate::{Driver, Executor, Result, Transaction};
use std::{
    borrow::Cow,
    future::{self, Future},
};

/// A live database handle capable of executing queries and spawning transactions.
///
/// This trait extends [`Executor`] adding functionality to acquire a connection
/// and to begin transactional scopes.
///
/// Drivers implement concrete `Connection` types to expose backend-specific
/// behavior (timeouts, pooling strategies, prepared statement caching, etc.).
///
/// # Lifecycle
/// - `connect` creates (or fetches) an underlying connection. It may eagerly
///   establish network I/O for validation; always await it.
/// - `begin` starts a transaction returning an object implementing
///   [`Transaction`]. Commit / rollback MUST be awaited to guarantee resource
///   release.
pub trait Connection: Executor {
    /// Create a connection (or pool) with at least one underlying session
    /// established to the given URL.
    fn connect(
        url: Cow<'static, str>,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Connection>>;

    /// Begin a transaction scope tied to the current connection.
    fn begin(&mut self) -> impl Future<Output = Result<impl Transaction<'_>>>;

    /// Disconnect and release the underlying session(s).
    ///
    /// Default implementation is a no-op; drivers may override to close sockets
    /// or return the connection to a pool asynchronously.
    fn disconnect(self) -> impl Future<Output = Result<()>> {
        future::ready(Ok(()))
    }
}
