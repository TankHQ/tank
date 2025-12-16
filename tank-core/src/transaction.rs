use crate::{Executor, Result};

/// Transactional `Executor` with `commit` and `rollback`.
pub trait Transaction<'c>: Executor {
    /// Commit the outstanding changes.
    fn commit(self) -> impl Future<Output = Result<()>>;
    /// Rollback any uncommitted changes.
    fn rollback(self) -> impl Future<Output = Result<()>>;
}
