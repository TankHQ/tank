#[cfg(test)]
mod tests {
use futures::executor::block_on;
use std::{
    any::Any,
    borrow::Cow,
    fmt,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};
use tank::{
    AsQuery, AsValue, Connection, Driver, Executor, GenericSqlWriter, Prepared, QueryResult, Result,
    RowOwned, RowsAffected, Transaction, anyhow,
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

    type Row<'s> = RowOwned;
    type Rows<'s, O>
        = TestRows<'s>
    where
        O: AsMut<tank::Query<Self>> + Send + 's;
    type Results<'s, O>
        = TestResults<'s>
    where
        O: AsMut<tank::Query<Self>> + Send + 's;

    fn sql_writer(&self) -> GenericSqlWriter {
        GenericSqlWriter::new()
    }
}

/// Rows of one test statement, counting each row as it is consumed.
struct TestRows<'s> {
    inner: std::vec::IntoIter<Result<RowOwned>>,
    consumed: &'s AtomicUsize,
}

impl Iterator for TestRows<'_> {
    type Item = Result<RowOwned>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.inner.next()?;
        self.consumed.fetch_add(1, Ordering::SeqCst);
        Some(row)
    }
}

/// Statement-level results of a test query.
struct TestResults<'s> {
    inner: std::vec::IntoIter<Result<QueryResult<std::vec::IntoIter<Result<RowOwned>>>>>,
    consumed: &'s AtomicUsize,
}

impl<'s> Iterator for TestResults<'s> {
    type Item = Result<QueryResult<TestRows<'s>>>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.inner.next()? {
            Ok(QueryResult::Rows(rows)) => Ok(QueryResult::Rows(TestRows {
                inner: rows,
                consumed: self.consumed,
            })),
            Ok(QueryResult::Affected(affected)) => Ok(QueryResult::Affected(affected)),
            Err(error) => Err(error),
        })
    }
}

#[derive(Debug, Default)]
struct TestExecutor {
    results: Vec<Result<QueryResult<std::vec::IntoIter<Result<RowOwned>>>>>,
    consumed: AtomicUsize,
}

impl Executor for TestExecutor {
    type Driver = TestDriver;

    fn run<'s, Q>(
        &'s mut self,
        _: Q,
    ) -> impl Future<Output = Result<<Self::Driver as Driver>::Results<'s, Q::Output>>> + Send
    where
        Q: AsQuery<TestDriver> + Send + 's,
    {
        let results = std::mem::take(&mut self.results);
        let consumed = &self.consumed;
        async move {
            Ok(TestResults {
                inner: results.into_iter(),
                consumed,
            })
        }
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
                    Ok(QueryResult::Rows(
                        vec![Ok(RowOwned::default()), Err(anyhow!("late row error"))].into_iter(),
                    )),
                    Err(anyhow!("late statement error")),
                    Ok(QueryResult::Rows(vec![Ok(RowOwned::default())].into_iter())),
                ],
                ..Default::default()
            };
            let mut rows = (&mut executor).fetch("").await.unwrap();
            assert!(rows.next().unwrap().is_ok());
            assert_eq!(
                rows.next().unwrap().err().unwrap().to_string(),
                "late row error"
            );
            assert_eq!(
                rows.next().unwrap().err().unwrap().to_string(),
                "late statement error"
            );
            assert!(rows.next().unwrap().is_ok());
            assert!(rows.next().is_none());
        });
    }

    #[test]
    fn execute_propagates_late_row_and_statement_errors() {
        block_on(async {
            for row_error in [true, false] {
                let mut rows = vec![Ok(RowOwned::default())];
                if row_error {
                    rows.push(Err(anyhow!("late row error")));
                }
                let mut executor = TestExecutor {
                    results: vec![
                        Ok(QueryResult::Affected(RowsAffected {
                            rows_affected: Some(2),
                            last_affected_id: Some(7),
                        })),
                        Ok(QueryResult::Rows(rows.into_iter())),
                    ],
                    ..Default::default()
                };
                if !row_error {
                    executor.results.push(Err(anyhow!("late statement error")));
                }
                let error = (&mut executor).execute("").await.unwrap_err();
                assert_eq!(
                    error.to_string(),
                    if row_error {
                        "late row error"
                    } else {
                        "late statement error"
                    }
                );
                assert_eq!(
                    executor.consumed.load(Ordering::SeqCst),
                    if row_error { 2 } else { 1 }
                );
            }
        });
    }

    #[test]
    fn execute_drains_rowsets_and_aggregates_effects() {
        block_on(async {
            let mut executor = TestExecutor {
                results: vec![
                    Ok(QueryResult::Rows(
                        vec![Ok(RowOwned::default()), Ok(RowOwned::default())].into_iter(),
                    )),
                    Ok(QueryResult::Affected(RowsAffected {
                        rows_affected: Some(2),
                        last_affected_id: Some(7),
                    })),
                    Ok(QueryResult::Rows(Vec::new().into_iter())),
                    Ok(QueryResult::Rows(vec![Ok(RowOwned::default())].into_iter())),
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
