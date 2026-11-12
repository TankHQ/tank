#[cfg(test)]
mod tests {
    use indoc::indoc;
    use std::fmt::Write;
    use tank::{
        Context, DynQuery, Entity, Expression, Fragment, GenericSqlWriter, QueryBuilder,
        QueryResult, QueryResultOwned, RawQuery, RowOwned, RowsAffected, SqlWriter, cols, expr,
    };

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn query_1() {
        #[derive(Entity)]
        struct MyTable {
            a: u32,
            b: i8,
            c: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(MyTable::columns())
                .from(MyTable::table()),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "a", "b", "c"
                FROM "my_table";
            "#}
            .trim()
        );
    }

    #[test]
    fn query_case() {
        #[derive(Entity)]
        struct Order {
            status: i32,
            amount: f64,
            note: Option<String>,
        }
        let render = |expression: &dyn Expression| {
            let mut sql = DynQuery::default();
            expression.write_query(
                &WRITER,
                &mut Context::new(Fragment::SqlSelect, false),
                &mut sql,
            );
            sql.as_str().into_owned()
        };
        assert_eq!(
            render(&expr!(if Order::amount > 100.0 {
                "large"
            } else if Order::note == NULL {
                "unknown"
            } else {
                "small"
            })),
            r#"CASE WHEN "amount" > 100.0 THEN 'large' WHEN "note" IS NULL THEN 'unknown' ELSE 'small' END"#,
        );
        assert_eq!(
            render(&expr!(if Order::status == 1 {
                "open"
            })),
            r#"CASE WHEN "status" = 1 THEN 'open' END"#,
        );
        let closed = 3;
        assert_eq!(
            render(&expr!(match Order::status {
                1 => "open",
                2 | #closed => "closed",
                -1 => "invalid",
                _ => "unknown",
            })),
            r#"CASE "status" WHEN 1 THEN 'open' WHEN 2 THEN 'closed' WHEN 3 THEN 'closed' WHEN -1 THEN 'invalid' ELSE 'unknown' END"#,
        );
        assert_eq!(
            render(&expr!(
                CAST(if Order::status == 0 {
                    "invalid"
                } else {
                    CAST(Order::status as String)
                } as i64)
                    + 1
            )),
            r#"CAST(CASE WHEN "status" = 0 THEN 'invalid' ELSE CAST("status" AS TEXT) END AS BIGINT) + 1"#,
        );
    }

    #[test]
    fn query_table_function() {
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(range))
                .from(expr!(range(3)))
                .where_expr(expr!(range > 0)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT range
                FROM range(3)
                WHERE range > 0;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_2() {
        #[derive(Entity)]
        #[tank(name = "the table")]
        struct TheTable {
            #[tank(name = "the_column")]
            some_col: i128,
            second_column: Box<String>,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select([TheTable::second_column, TheTable::some_col])
                .from(TheTable::table())
                .where_expr(expr!(
                    TheTable::second_column == "So%" as LIKE
                        || TheTable::some_col >= 0 && TheTable::some_col < 10
                )),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "second_column", "the_column"
                FROM "the table"
                WHERE "second_column" LIKE 'So%' OR "the_column" >= 0 AND "the_column" < 10;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_3() {
        #[derive(Entity)]
        struct Orders {
            customer_id: u32,
            amount: f64,
            status: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(Orders::customer_id, SUM(Orders::amount)))
                .from(Orders::table())
                .where_expr(expr!(Orders::status == "completed"))
                .group_by([Orders::customer_id]),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "customer_id", SUM("amount")
                FROM "orders"
                WHERE "status" = 'completed'
                GROUP BY "customer_id";
            "#}
            .trim()
        );

        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(Orders::customer_id as cid, SUM(Orders::amount)))
                .from(Orders::table())
                .group_by(cols!(Orders::customer_id as cid)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "customer_id" AS "cid", SUM("amount")
                FROM "orders"
                GROUP BY "customer_id";
            "#}
            .trim()
        );
    }

    #[test]
    fn query_4() {
        #[derive(Entity)]
        struct Sales {
            region: String,
            product: String,
            revenue: f64,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(Sales::region, SUM(Sales::revenue)))
                .from(Sales::table())
                .where_expr(expr!(Sales::revenue > 0))
                .group_by([Sales::region])
                .having(expr!(SUM(Sales::revenue) > 1000)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "region", SUM("revenue")
                FROM "sales"
                WHERE "revenue" > 0
                GROUP BY "region"
                HAVING SUM("revenue") > 1000;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_5() {
        #[derive(Entity)]
        struct Products {
            name: String,
            price: f64,
            category: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Products::columns())
                .from(Products::table())
                .where_expr(expr!(Products::category == "electronics"))
                .order_by(cols!(Products::price ASC)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "name", "price", "category"
                FROM "products"
                WHERE "category" = 'electronics'
                ORDER BY "price" ASC;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_6() {
        #[derive(Entity)]
        struct Events {
            id: u64,
            timestamp: i64,
            level: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Events::columns())
                .from(Events::table())
                .order_by(cols!(Events::timestamp DESC))
                .limit(Some(50)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "id", "timestamp", "level"
                FROM "events"
                ORDER BY "timestamp" DESC
                LIMIT 50;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_7() {
        #[derive(Entity)]
        struct Employees {
            department: String,
            name: String,
            salary: f64,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Employees::columns())
                .from(Employees::table())
                .order_by(cols!(Employees::department ASC, Employees::salary DESC)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "department", "name", "salary"
                FROM "employees"
                ORDER BY "department" ASC, "salary" DESC;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_8() {
        #[derive(Entity)]
        struct Transactions {
            account_id: u32,
            category: String,
            amount: f64,
            created_at: i64,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(
                    Transactions::account_id,
                    Transactions::category,
                    SUM(Transactions::amount),
                    COUNT(*)
                ))
                .from(Transactions::table())
                .where_expr(expr!(Transactions::created_at > 1700000000))
                .group_by(cols!(Transactions::account_id, Transactions::category))
                .having(expr!(COUNT(*) > 5))
                .order_by(cols!(SUM(Transactions::amount) DESC))
                .limit(Some(100)),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "account_id", "category", SUM("amount"), COUNT(*)
                FROM "transactions"
                WHERE "created_at" > 1700000000
                GROUP BY "account_id", "category"
                HAVING COUNT(*) > 5
                ORDER BY SUM("amount") DESC
                LIMIT 100;
            "#}
            .trim()
        );
    }

    #[test]
    fn query_9() {
        #[derive(Entity)]
        struct Metrics {
            region: String,
            service: String,
            latency_ms: f64,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(cols!(
                    Metrics::region,
                    Metrics::service,
                    AVG(Metrics::latency_ms)
                ))
                .from(Metrics::table())
                .group_by([Metrics::region, Metrics::service]),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "region", "service", AVG("latency_ms")
                FROM "metrics"
                GROUP BY "region", "service";
            "#}
            .trim()
        );
    }

    #[test]
    fn query_create_table() {
        #[derive(Entity)]
        struct Widget {
            id: u32,
            name: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_create_table::<Widget>(&mut sql, false);
        let result = sql.as_str();
        assert!(result.contains("CREATE TABLE"));
        assert!(result.contains("\"widget\""));
        let mut sql = DynQuery::default();
        WRITER.write_create_table::<Widget>(&mut sql, true);
        assert!(sql.as_str().contains("IF NOT EXISTS"));
    }

    #[test]
    fn query_drop_table() {
        #[derive(Entity)]
        struct Widget {
            id: u32,
        }
        let mut sql = DynQuery::default();
        WRITER.write_drop_table::<Widget>(&mut sql, false);
        assert!(sql.as_str().contains("DROP TABLE"));
        let mut sql = DynQuery::default();
        WRITER.write_drop_table::<Widget>(&mut sql, true);
        assert!(sql.as_str().contains("IF EXISTS"));
    }

    #[test]
    fn query_insert() {
        #[derive(Entity)]
        struct Item {
            id: u32,
            label: String,
        }
        let items = vec![
            Item {
                id: 1,
                label: "a".into(),
            },
            Item {
                id: 2,
                label: "b".into(),
            },
        ];
        let mut sql = DynQuery::default();
        WRITER.write_insert(&mut sql, &items, false);
        let result = sql.as_str();
        assert!(result.contains("INSERT INTO"));
        assert!(result.contains("\"item\""));
        let mut sql = DynQuery::default();
        WRITER.write_insert(&mut sql, &items, true);
        assert!(sql.as_str().contains("INSERT INTO"));
    }

    #[test]
    fn query_limit() {
        #[derive(Entity)]
        struct Limitable {
            x: u32,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Limitable::columns())
                .from(Limitable::table())
                .order_by(cols!(Limitable::x ASC))
                .limit(Some(10)),
        );
        assert!(sql.as_str().contains("LIMIT 10"));
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Limitable::columns())
                .from(Limitable::table())
                .order_by(cols!(Limitable::x ASC))
                .limit(None),
        );
        assert!(!sql.as_str().contains("LIMIT"));
    }

    #[test]
    fn query_18() {
        #[derive(Entity)]
        struct Tbl {
            col_a: u32,
            col_b: String,
        }
        let gb = cols!(Tbl::col_a);
        let ob = cols!(Tbl::col_a ASC);
        let q = QueryBuilder::new()
            .select(Tbl::columns())
            .from(Tbl::table())
            .where_expr(expr!(Tbl::col_a > 0))
            .group_by(gb)
            .having(expr!(Tbl::col_b != NULL))
            .order_by(ob)
            .limit(Some(5));

        // Exercise all getters
        let _sel: Vec<_> = q.get_select().collect();
        assert!(q.get_from().is_some());
        assert!(q.get_where().is_some());
        let _gb: Vec<_> = q.get_group_by().collect();
        assert!(q.get_having().is_some());
        let _ob: Vec<_> = q.get_order_by().collect();
        assert_eq!(q.get_limit(), Some(5));
    }

    #[test]
    fn query_builder_getters() {
        #[derive(Entity)]
        struct Foo {
            id: u32,
        }
        assert!(!QueryBuilder::new().create_table::<Foo>().get_not_exists());
        assert!(
            QueryBuilder::new()
                .create_table::<Foo>()
                .if_not_exists()
                .get_not_exists()
        );
        assert!(!QueryBuilder::new().drop_table::<Foo>().get_exists());
        assert!(
            QueryBuilder::new()
                .drop_table::<Foo>()
                .if_exists()
                .get_exists()
        );
        let rows = vec![Foo { id: 1 }];
        assert!(!QueryBuilder::new().insert_into().values(&rows).get_update());
        assert!(
            QueryBuilder::new()
                .insert_into()
                .values(&rows)
                .update()
                .get_update()
        );
        assert!(
            QueryBuilder::new()
                .delete_from(Foo::table())
                .get_where()
                .is_none()
        );
        assert!(
            QueryBuilder::new()
                .delete_from(Foo::table())
                .where_expr(expr!(Foo::id == 1))
                .get_where()
                .is_some()
        );
    }

    #[test]
    fn raw_query_helpers() {
        let rq = RawQuery("SELECT * FROM t".to_string());
        assert_eq!(format!("{rq}"), "SELECT * FROM t");
        let mut rq = RawQuery("SELECT ?".to_string());
        assert_eq!(rq.0, "SELECT ?");
        rq.0.clear();
        assert!(rq.0.is_empty());

        let rq = RawQuery("hello".to_string());
        assert_eq!(rq.0, "hello");

        let mut q = DynQuery::default();
        let _ = q.write_str("SELECT 1");
        let s: String = q.into();
        assert_eq!(s, "SELECT 1");

        let row = tank::RowOwned::default();
        let QueryResult::Row(_row) = QueryResult::Row(row) else {
            unreachable!()
        };
        assert!(matches!(
            QueryResult::<tank::RowOwned>::from(RowsAffected::default()),
            QueryResult::Affected(_)
        ));
    }

    #[test]
    fn owned_results() {
        let results: Vec<QueryResultOwned> = vec![
            QueryResult::Affected(RowsAffected {
                rows_affected: Some(2),
                last_affected_id: Some(7),
            }),
            QueryResult::Row(RowOwned::default()),
            QueryResult::Row(RowOwned::default()),
        ];
        assert_eq!(results.len(), 3);
        assert!(matches!(&results[0], QueryResult::Affected(_)));
        assert!(matches!(&results[1], QueryResult::Row(_)));
        assert_eq!(
            results
                .into_iter()
                .filter_map(|result| match result {
                    QueryResult::Row(row) => Some(row),
                    QueryResult::Affected(_) => None,
                })
                .count(),
            2,
        );
    }

    #[test]
    fn affected_aggregation() {
        let results = [
            QueryResult::Affected(RowsAffected {
                rows_affected: Some(2),
                last_affected_id: Some(7),
            }),
            QueryResult::Row(0),
            QueryResult::Row(1),
            QueryResult::Affected(RowsAffected::default()),
            QueryResult::Affected(RowsAffected {
                rows_affected: Some(4),
                last_affected_id: Some(9),
            }),
        ];
        let mut affected = RowsAffected::default();
        for result in results {
            if let QueryResult::Affected(result) = result {
                affected.extend([result]);
            }
        }
        assert_eq!(affected.rows_affected, Some(6));
        assert_eq!(affected.last_affected_id, Some(9));
    }
}
