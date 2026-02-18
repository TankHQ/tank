use crate::{Driver, Error, Executor, Result, truncate_long};
use anyhow::Context;
use std::{
    borrow::Cow,
    future::{self, Future},
};
use url::Url;

/// A live database handle capable of executing queries and spawning transactions.
///
/// Extends [`Executor`] with connection and transaction management.
///
/// # Lifecycle
/// - `connect` creates (or fetches) an underlying connection. It may eagerly
///   establish network I/O; always await it.
/// - `begin` starts a transaction scope. Commit/rollback MUST be awaited to
///   guarantee resource release.
pub trait Connection: Executor {
    fn sanitize_url(mut url: Cow<'static, str>) -> Result<Url>
    where
        Self: Sized,
    {
        let mut in_memory = false;
        if let Some((scheme, host)) = url.split_once("://")
            && host.starts_with(":memory:")
        {
            url = format!("{scheme}://localhost{}", &host[8..]).into();
            in_memory = true;
        }
        let context = || format!("While trying to connect to `{}`", truncate_long!(url));
        let mut result = Url::parse(&url).with_context(context)?;
        if in_memory {
            result.query_pairs_mut().append_pair("mode", "memory");
        }
        let names = <Self::Driver as Driver>::NAME;
        'prefix: {
            for name in names {
                let prefix = format!("{}://", name);
                if url.starts_with(&prefix) {
                    break 'prefix prefix;
                }
            }
            let error = Error::msg(format!(
                "Connection URL must start with: {}",
                names.join(", ")
            ))
            .context(context());
            log::error!("{:#}", error);
            return Err(error);
        };
        Ok(result)
    }

    /// Create a connection (or pool) to the given URL.
    ///
    /// Implementations may perform I/O or validation during `connect`.
    /// Callers should treat this as a potentially expensive operation.
    fn connect(
        url: Cow<'static, str>,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Connection>>
    where
        Self: Sized;

    /// Begin a transaction scope tied to this connection.
    ///
    /// Must await `commit` or `rollback` to finalize the scope and release resources.
    fn begin(&mut self) -> impl Future<Output = Result<<Self::Driver as Driver>::Transaction<'_>>>;

    /// Disconnect and release the underlying session(s).
    fn disconnect(self) -> impl Future<Output = Result<()>>
    where
        Self: Sized,
    {
        future::ready(Ok(()))
    }
}
