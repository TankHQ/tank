#[cfg(test)]
mod tests {
use futures::executor::block_on;
use std::{
    any::Any,
    borrow::Cow,
    fmt,
    pin::pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tank::{
    AsQuery, AsValue, Connection, Driver, Executor, GenericSqlWriter, Prepared, QueryResult, Result,
    RowOwned, RowsAffected, Transaction, anyhow,
    stream::{self, Stream, StreamExt},
};

#[derive(Clone, Debug, Default)]
struct TestDriver;

impl fmt::Display for TestDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test")
    }
}

impl Prepared for TestDriver {
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        Ok(self)
    }
    fn bind(&mut self, _: impl AsValue) -> Result<&mut Self> {
        Ok(self)
    }
    fn bind_index(&mut self, _: impl AsValue, _: u64) -> Result<&mut Self> {
        Ok(self)
    }
}

impl Driver for TestDriver {
    type Connection = TestExecutor;
    type SqlWriter = GenericSqlWriter;
    type Prepared = Self;
    type Transaction<'c> = TestExecutor;
    const NAME: &'static [&'static str] = &["test"];

    fn sql_writer(&self) -> GenericSqlWriter {
        GenericSqlWriter::new()
    }
}

/// Flat stream results of a test query, counting each row as it is consumed.
struct TestResults<'s> {
    inner: std::vec::IntoIter<Result<QueryResult<RowOwned>>>,
    consumed: &'s AtomicUsize,
}

impl Iterator for TestResults<'_> {
    type Item = Result<QueryResult<RowOwned>>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if matches!(item, Ok(QueryResult::Row(_))) {
            self.consumed.fetch_add(1, Ordering::SeqCst);
        }
        Some(item)
    }
}

#[derive(Debug, Default)]
struct TestExecutor {
    results: Vec<Result<QueryResult<RowOwned>>>,
    consumed: AtomicUsize,
}

impl Executor for TestExecutor {
    type Driver = TestDriver;

    fn run<'s>(
        &'s mut self,
        _: impl AsQuery<TestDriver> + 's,
    ) -> impl Stream<Item = Result<QueryResult<impl tank::Row + 's>>> + Send {
        let results = std::mem::take(&mut self.results);
        let consumed = &self.consumed;
        stream::iter(TestResults {
            inner: results.into_iter(),
            consumed,
        })
    }
}

    impl Connection for TestExecutor {
        async fn connect(_: &TestDriver, _: Cow<'static, str>) -> Result<Self> {
            Ok(Self::default())
        }
        async fn begin(&mut self) -> Result<Self> {
            Ok(Self::default())
        }
    }

    impl Transaction<'_> for TestExecutor {
        async fn commit(self) -> Result<()> {
            Ok(())
        }
        async fn rollback(self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn fetch_preserves_late_row_and_statement_errors() {
        block_on(async {
            let mut executor = TestExecutor {
                results: vec![
                    Ok(QueryResult::Affected(RowsAffected::default())),
                    Ok(QueryResult::Row(RowOwned::default())),
                    Err(anyhow!("Late row error")),
                    Err(anyhow!("Late statement error")),
                    Ok(QueryResult::Row(RowOwned::default())),
                ],
                ..Default::default()
            };
            let mut rows = pin!((&mut executor).fetch(""));
            assert!(rows.next().await.unwrap().is_ok());
            assert_eq!(
                rows.next().await.unwrap().err().unwrap().to_string(),
                "Late row error"
            );
            assert_eq!(
                rows.next().await.unwrap().err().unwrap().to_string(),
                "Late statement error"
            );
            assert!(rows.next().await.unwrap().is_ok());
            assert!(rows.next().await.is_none());
        });
    }

    #[test]
    fn execute_propagates_late_row_and_statement_errors() {
        block_on(async {
            for row_error in [true, false] {
                let mut results = vec![
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: Some(2),
                        last_affected_id: Some(7),
                    })),
                    Ok(QueryResult::Row(RowOwned::default())),
                ];
                if row_error {
                    results.push(Err(anyhow!("Late row error")));
                } else {
                    results.push(Err(anyhow!("Late statement error")));
                }
                let mut executor = TestExecutor {
                    results,
                    ..Default::default()
                };
                let error = (&mut executor).execute("").await.unwrap_err();
                assert_eq!(
                    error.to_string(),
                    if row_error {
                        "Late row error"
                    } else {
                        "Late statement error"
                    }
                );
                assert_eq!(executor.consumed.load(Ordering::SeqCst), 1);
            }
        });
    }

    #[test]
    fn execute_drains_rowsets_and_aggregates_effects() {
        block_on(async {
            let mut executor = TestExecutor {
                results: vec![
                    Ok(QueryResult::Row(RowOwned::default())),
                    Ok(QueryResult::Row(RowOwned::default())),
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: Some(2),
                        last_affected_id: Some(7),
                    })),
                    Ok(QueryResult::Row(RowOwned::default())),
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: Some(4),
                        last_affected_id: Some(9),
                    })),
                ],
                ..Default::default()
            };
            let affected = executor.execute("").await.unwrap();
            assert_eq!(executor.consumed.load(Ordering::SeqCst), 3);
            assert_eq!(affected.rows_affected, Some(6));
            assert_eq!(affected.last_affected_id, Some(9));
        });
    }
}
