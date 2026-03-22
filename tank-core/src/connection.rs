use crate::{Driver, Error, Executor, Result};
use anyhow::Context;
use convert_case::{Case, Casing};
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
    /// Validates and normalizes the connection URL, handling special cases like in-memory databases.
    fn sanitize_url(driver: &Self::Driver, mut url: Cow<'static, str>) -> Result<Url>
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
        let context = || {
            format!(
                "While trying to connect to {}",
                driver.name().to_case(Case::Title)
            )
        };
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

    /// Establishes a connection (or pool) to the database specified by the URL.
    ///
    /// Implementations may perform I/O or validation during `connect`.
    /// Callers should treat this as a potentially expensive operation.
    fn connect(
        driver: &Self::Driver,
        url: Cow<'static, str>,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Connection>>
    where
        Self: Sized;

    /// Starts a new transaction on this connection.
    ///
    /// Must await `commit` or `rollback` to finalize the scope and release resources.
    fn begin(&mut self) -> impl Future<Output = Result<<Self::Driver as Driver>::Transaction<'_>>>;

    /// Closes the connection and releases any session resources.
    fn disconnect(self) -> impl Future<Output = Result<()>>
    where
        Self: Sized,
    {
        future::ready(Ok(()))
    }
}
