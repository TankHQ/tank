#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use indoc::indoc;
    use std::{
        any::Any,
        borrow::Cow,
        fmt,
        future::Future,
        mem,
        pin::pin,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
        vec::IntoIter,
    };
    use tank::{
        AsQuery, AsValue, Connection, ConnectionPool, Context, Dataset, Driver, DynQuery, Entity,
        Error, Executor, Expression, ExpressionCollection, ExpressionVisitor, Fragment,
        GenericSqlWriter, InsertIntoQuery, NA, OpPrecedence, PoolConfig, PooledConnection,
        Prepared, Query, QueryResult, RawQuery, Result, Row, RowOwned, RowsAffected, Sql,
        Transaction, anyhow, cols, describe_path, describe_url, expr,
        future::Either,
        stream::{self, Stream, StreamExt, TryStreamExt},
    };

    #[derive(Clone, Debug, Default, Entity, PartialEq)]
    #[tank(schema = "s", name = "executor_items")]
    struct ExecutorItem {
        #[tank(primary_key)]
        id: i32,
        /// A documented column
        name: String,
    }

    #[derive(Debug, Default, Entity)]
    #[tank(name = "keyless")]
    struct Keyless {
        value: i32,
    }

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
        fn is_empty(&self) -> bool {
            false
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
        inner: IntoIter<Result<QueryResult<RowOwned>>>,
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

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
        }
    }

    #[derive(Debug, Default)]
    struct TestExecutor {
        results: Vec<Result<QueryResult<RowOwned>>>,
        consumed: AtomicUsize,
        single_statement: bool,
    }

    impl Executor for TestExecutor {
        type Driver = TestDriver;

        fn accepts_multiple_statements(&self) -> bool {
            !self.single_statement
        }

        fn run<'s>(
            &'s mut self,
            query: impl AsQuery<TestDriver> + 's,
        ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
            let _ = query.as_query();
            let results = mem::take(&mut self.results);
            let consumed = &self.consumed;
            stream::iter(TestResults {
                inner: results.into_iter(),
                consumed,
            })
        }

        fn do_prepare(
            &mut self,
            _: String,
        ) -> impl Future<Output = Result<Query<TestDriver>>> + Send {
            async move { Ok(Query::prepared(TestDriver)) }
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
    fn fetch_late_errors() {
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
            assert_eq!(rows.size_hint(), (5, Some(5)));
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
    fn execute_late_errors() {
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
    fn execute_aggregates_effects() {
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

    #[test]
    fn stream_try_collect_and_forwarders() {
        block_on(async {
            let collected: Vec<i32> = stream::iter(vec![Ok::<_, Error>(1), Ok(2)])
                .map_ok(|v| v * 2)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(collected, vec![2, 4]);

            let err = stream::iter(vec![Ok(1), Err(anyhow!("boom"))])
                .try_collect::<Vec<i32>>()
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), "boom");

            let mut s = stream::iter(vec![Ok::<_, Error>(1)]);
            assert_eq!(s.try_next().await.unwrap(), Some(1));
        });
    }

    #[test]
    fn query_and_builder_conversions() {
        let mut raw: Query<TestDriver> = "SELECT 1".into();
        assert!(!raw.is_prepared());
        assert_eq!(raw.to_string(), "SELECT 1");
        assert!(raw.clear_bindings().is_ok());
        assert!(raw.bind(1_i32).is_err());
        assert!(raw.bind_index(1_i32, 0).is_err());
        assert!(!Query::<TestDriver>::default().is_prepared());
        assert_eq!(Query::<TestDriver>::from("a".to_string()).to_string(), "a");

        let mut prepared: Query<TestDriver> = TestDriver.into();
        assert!(prepared.is_prepared());
        prepared.bind(1_i32).unwrap().bind_index(2_i32, 0).unwrap();
        prepared.clear_bindings().unwrap();
        assert_eq!(prepared.to_string(), "test");

        assert_eq!(
            AsQuery::<TestDriver>::as_query(RawQuery("z".into()))
                .as_mut()
                .to_string(),
            "z"
        );
        assert_eq!(
            AsQuery::<TestDriver>::as_query(Query::raw("v".into()))
                .as_mut()
                .to_string(),
            "v"
        );

        let matching: DynQuery = Query::<TestDriver>::prepared(TestDriver).into();
        assert!(AsQuery::<TestDriver>::as_query(matching).is_prepared());
        let mut dyn_q: DynQuery = prepared.into_dyn();
        assert!(dyn_q.as_str().contains("TestDriver"));
        assert_eq!(dyn_q.len(), 0);
        assert!(!dyn_q.is_empty());
        assert!(dyn_q.as_prepared::<TestDriver>().is_some());
        assert!(
            String::from(DynQuery::from(Query::<TestDriver>::prepared(TestDriver)))
                .contains("TestDriver")
        );
        let back: Query<TestDriver> = dyn_q.into();
        assert!(back.is_prepared());

        let mut dyn_prep: DynQuery = Query::<TestDriver>::prepared(TestDriver).into();
        assert_eq!(dyn_prep.buffer().as_str(), "");
        assert!(dyn_prep.as_prepared::<TestDriver>().is_none());
        let mut dyn_prep: DynQuery = Query::<TestDriver>::prepared(TestDriver).into();
        dyn_prep.clear();
        assert_eq!(dyn_prep.into_query(TestDriver).to_string(), "");

        #[derive(Debug)]
        struct OtherPrepared;
        impl fmt::Display for OtherPrepared {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("other")
            }
        }
        impl Prepared for OtherPrepared {
            fn as_any(self: Box<Self>) -> Box<dyn Any> {
                self
            }
            fn is_empty(&self) -> bool {
                true
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
        let mut other = DynQuery::Prepared(Box::new(OtherPrepared));
        assert!(other.is_empty());
        assert!(other.as_prepared::<TestDriver>().is_none());
        assert!(!Query::<TestDriver>::from(other).is_prepared());

        struct Visitor;
        impl ExpressionVisitor for Visitor {}
        let na = NA;
        assert_eq!(na.precedence(&GenericSqlWriter::new()), 0);
        let mut out = DynQuery::default();
        na.write_query(&GenericSqlWriter::new(), &mut Context::empty(), &mut out);
        assert!(!na.accept_visitor(
            &mut Visitor,
            &GenericSqlWriter::new(),
            &mut Context::empty(),
            &mut out
        ));
        assert_eq!(na.expr_iter().count(), 0);

        let create = Sql().create_table::<ExecutorItem>();
        assert!(!create.build(&TestDriver).is_empty());
        create.build_into(&TestDriver, &mut out);
        let drop = Sql().drop_table::<ExecutorItem>();
        assert!(!drop.build(&TestDriver).is_empty());
        drop.build_into(&TestDriver, &mut out);
        let rows = [ExecutorItem::default()];
        let insert = Sql().insert_into(ExecutorItem::table()).values(&rows);
        let Either::Left(values) = insert.get_values() else {
            panic!("expected rows of values");
        };
        assert_eq!(values.count(), 1);
        assert!(!insert.build(&TestDriver).is_empty());
        insert.build_into(&TestDriver, &mut out);
        let upsert = Sql()
            .insert_into(ExecutorItem::table())
            .values(&rows)
            .update();
        assert!(upsert.build(&TestDriver).as_str().contains("ON CONFLICT"));
        assert_eq!(
            Sql()
                .delete_from("logs")
                .where_expr(expr!(level == "debug"))
                .build(&TestDriver)
                .as_str(),
            "DELETE FROM \"logs\"\nWHERE level = 'debug';"
        );
        let delete = Sql().delete_from(ExecutorItem::table());
        assert!(
            delete
                .build(&TestDriver)
                .as_str()
                .starts_with("DELETE FROM")
        );
        delete.build_into(&TestDriver, &mut out);
        let select = Sql()
            .select(ExecutorItem::columns())
            .from(ExecutorItem::table());
        assert!(!select.build(&TestDriver).is_empty());
        select.build_into(&TestDriver, &mut out);
    }

    #[test]
    fn insert_without_entities() {
        // Table by name, columns by name, rows of values
        assert_eq!(
            Sql()
                .insert_into("mytable")
                .columns(["col_a", "col_b"])
                .values([
                    ["val_a_1".as_value(), 1_i32.as_value()],
                    ["val_a_2".as_value(), 2_i32.as_value()],
                ])
                .build(&TestDriver)
                .as_str(),
            indoc! {r#"
                INSERT INTO "mytable" ("col_a", "col_b") VALUES
                ('val_a_1', 1),
                ('val_a_2', 2);
            "#}
            .trim()
        );

        // Schema and name, rows built at runtime with a NULL
        let rows = (1..=2)
            .map(|i| {
                vec![
                    i.as_value(),
                    format!("event {i}").as_value(),
                    None::<f64>.as_value(),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(
            Sql()
                .insert_into(("analytics", "events"))
                .columns(["id", "name", "score"])
                .values(&rows)
                .build(&TestDriver)
                .as_str(),
            indoc! {r#"
                INSERT INTO "analytics"."events" ("id", "name", "score") VALUES
                (1, 'event 1', NULL),
                (2, 'event 2', NULL);
            "#}
            .trim()
        );

        // Values of one type need no conversion
        assert_eq!(
            Sql()
                .insert_into("points")
                .columns(["x", "y"])
                .values([[1, 2], [3, 4]])
                .build(&TestDriver)
                .as_str(),
            indoc! {r#"
                INSERT INTO "points" ("x", "y") VALUES
                (1, 2),
                (3, 4);
            "#}
            .trim()
        );

        // No rows, no statement
        assert!(
            Sql()
                .insert_into("mytable")
                .columns(["col_a"])
                .values(Vec::<[tank::Value; 1]>::new())
                .build(&TestDriver)
                .is_empty()
        );

        // Rows carrying their column names
        let labels: tank::RowLabels = ["level", "message"].map(String::from).into();
        assert_eq!(
            Sql()
                .insert_into("logs")
                .columns(labels.iter().cloned())
                .values(
                    [RowOwned::new(
                        labels.clone(),
                        vec!["debug".as_value(), "started".as_value()]
                    )]
                    .iter()
                    .map(Row::values)
                )
                .build(&TestDriver)
                .as_str(),
            indoc! {r#"
                INSERT INTO "logs" ("level", "message") VALUES
                ('debug', 'started');
            "#}
            .trim()
        );

        // The result of a query, tables and columns by name
        assert_eq!(
            Sql()
                .insert_into(("analytics", "archive"))
                .select(cols!(id, name))
                .from(("analytics", "events"))
                .where_expr(expr!(score == NULL))
                .build(&TestDriver)
                .as_str(),
            indoc! {r#"
                INSERT INTO "analytics"."archive"
                SELECT id, name
                FROM "analytics"."events"
                WHERE score IS NULL;
            "#}
            .trim()
        );
    }

    #[test]
    fn entity_operations() {
        block_on(async {
            for single_statement in [false, true] {
                let mut exec = TestExecutor {
                    single_statement,
                    ..Default::default()
                };
                ExecutorItem::create_table(&mut exec, true, true)
                    .await
                    .unwrap();
                ExecutorItem::drop_table(&mut exec, true, true)
                    .await
                    .unwrap();
            }

            let item = ExecutorItem {
                id: 1,
                name: "a".into(),
            };
            let mut exec = TestExecutor::default();
            ExecutorItem::insert_one(&mut exec, &item).await.unwrap();
            ExecutorItem::insert_many(&mut exec, [&item]).await.unwrap();
            assert!(
                ExecutorItem::prepare_find(&mut exec, true, None)
                    .await
                    .unwrap()
                    .is_prepared()
            );
            assert!(
                ExecutorItem::find_one(&mut exec, true)
                    .await
                    .unwrap()
                    .is_none()
            );
            exec.results.push(Ok(QueryResult::Row(item.row())));
            assert_eq!(
                ExecutorItem::find_one(&mut exec, true)
                    .await
                    .unwrap()
                    .unwrap(),
                item
            );
            ExecutorItem::delete_many(&mut exec, true).await.unwrap();

            exec.results.push(Ok(QueryResult::Affected(RowsAffected {
                rows_affected: Some(1),
                last_affected_id: None,
            })));
            item.save(&mut exec).await.unwrap();
            exec.results.push(Ok(QueryResult::Affected(RowsAffected {
                rows_affected: Some(3),
                last_affected_id: None,
            })));
            assert!(item.save(&mut exec).await.is_err());
            assert!(Keyless { value: 1 }.save(&mut exec).await.is_err());

            exec.results.push(Ok(QueryResult::Affected(RowsAffected {
                rows_affected: Some(1),
                last_affected_id: None,
            })));
            item.delete(&mut exec).await.unwrap();
            for rows_affected in [Some(0), Some(2)] {
                exec.results.push(Ok(QueryResult::Affected(RowsAffected {
                    rows_affected,
                    last_affected_id: None,
                })));
                assert!(item.delete(&mut exec).await.is_err());
            }
            assert!(Keyless { value: 1 }.delete(&mut exec).await.is_err());

            let mut out = DynQuery::default();
            item.write_table_name(
                &GenericSqlWriter::new(),
                &mut Context::fragment(Fragment::SqlSelectFrom),
                &mut out,
            );
            assert_eq!(Dataset::table_ref(&item), *ExecutorItem::table());
        });
    }

    #[test]
    fn pool_and_connection_helpers() {
        block_on(async {
            struct NoPrepare(TestExecutor);
            impl Executor for NoPrepare {
                type Driver = TestDriver;
                fn run<'s>(
                    &'s mut self,
                    _: impl AsQuery<TestDriver> + 's,
                ) -> impl Stream<Item = Result<QueryResult<impl Row + 's>>> + Send {
                    self.0.run("")
                }
            }
            let mut no_prep = NoPrepare(TestExecutor::default());
            assert!(no_prep.accepts_multiple_statements());
            assert!(no_prep.prepare("SELECT 1").await.is_err());

            let mut exec = TestExecutor::default();
            let mut already = Query::<TestDriver>::prepared(TestDriver);
            assert!(exec.prepare(&mut already).await.unwrap().is_prepared());
            let mut exec_ref = &mut exec;
            assert!(Executor::accepts_multiple_statements(&&mut exec_ref));
            assert_eq!(Executor::driver(&&mut exec_ref).name(), "test");
            assert!(
                Executor::do_prepare(&mut exec_ref, "SELECT 1".to_string())
                    .await
                    .unwrap()
                    .is_prepared()
            );
            {
                let mut stream = pin!(Executor::run(&mut exec_ref, ""));
                assert!(stream.next().await.is_none());
            }
            assert!(Connection::disconnect(exec_ref).await.is_err());
            exec.disconnect().await.unwrap();

            for (raw, expected_path) in [
                ("test://:memory:", ":memory:"),
                ("test://host?path=/tmp/a.db", "/tmp/a.db"),
                ("test:///", "N/A"),
                ("test:///tmp/a.db", "/tmp/a.db"),
            ] {
                let url = TestExecutor::sanitize_url(&TestDriver, raw.into()).unwrap();
                assert_eq!(describe_path(&url), expected_path);
            }
            let url = TestExecutor::sanitize_url(
                &TestDriver,
                "test://user:pw@db.example.com:5432/orders".into(),
            )
            .unwrap();
            assert_eq!(
                describe_url::<TestDriver>(&url),
                "db.example.com:5432/orders"
            );
            assert!(TestExecutor::sanitize_url(&TestDriver, "other://host".into()).is_err());

            let config = PoolConfig {
                max_size: 1,
                ..PoolConfig::default()
            };
            let pool = TestDriver
                .connect_pool("test://localhost", config)
                .await
                .unwrap();
            let mut conn = pool.get().await.unwrap();
            assert!(conn.accepts_multiple_statements());
            assert_eq!(conn.driver().name(), "test");
            let _ = &*conn;
            let _ = conn.as_ref();
            let _ = conn.as_mut();
            assert!(conn.prepare("SELECT 1").await.unwrap().is_prepared());
            assert_eq!(conn.execute("").await.unwrap().rows_affected, None);
            assert!(
                conn.fetch("")
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty()
            );
            let _ = conn.begin().await.unwrap();
            assert!(
                PooledConnection::<TestDriver>::connect(&TestDriver, "test://localhost".into())
                    .await
                    .is_err()
            );
            assert!(pool.timeout_get(Duration::from_millis(1)).await.is_err());
            drop(conn);
            let _ = pool.detach().await.unwrap();
            pool.resize(2).unwrap();
            let _ = pool.into_arc();
            let pool2 = TestDriver
                .connect_pool(String::from("test://localhost"), PoolConfig::new())
                .await
                .unwrap();
            let _ = pool2.into_box();
            let pool3 = TestDriver
                .connect_pool(Cow::Borrowed("test://localhost"), PoolConfig::new())
                .await
                .unwrap();
            pool3.close().await.unwrap();
        });
    }
}
