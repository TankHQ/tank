#[cfg(test)]
mod tests {
    use indoc::indoc;
    use tank::{DynQuery, Entity, GenericSqlWriter, QueryBuilder, SqlWriter, cols, expr};

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
    fn query_10() {
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
    }

    #[test]
    fn query_11() {
        #[derive(Entity)]
        struct Widget {
            id: u32,
            name: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_create_table::<Widget>(&mut sql, true);
        let result = sql.as_str();
        assert!(result.contains("IF NOT EXISTS"));
    }

    #[test]
    fn query_12() {
        #[derive(Entity)]
        struct Widget {
            id: u32,
        }
        let mut sql = DynQuery::default();
        WRITER.write_drop_table::<Widget>(&mut sql, false);
        assert!(sql.as_str().contains("DROP TABLE"));
    }

    #[test]
    fn query_13() {
        #[derive(Entity)]
        struct Widget {
            id: u32,
        }
        let mut sql = DynQuery::default();
        WRITER.write_drop_table::<Widget>(&mut sql, true);
        assert!(sql.as_str().contains("IF EXISTS"));
    }

    #[test]
    fn query_14() {
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
        WRITER.write_insert::<Item>(&mut sql, &items, false);
        let result = sql.as_str();
        assert!(result.contains("INSERT INTO"));
        assert!(result.contains("\"item\""));
    }

    #[test]
    fn query_15() {
        #[derive(Entity)]
        struct Item {
            id: u32,
            label: String,
        }
        let items = vec![Item {
            id: 1,
            label: "x".into(),
        }];
        let mut sql = DynQuery::default();
        WRITER.write_insert::<Item>(&mut sql, &items, true);
        let result = sql.as_str();
        assert!(result.contains("INSERT INTO"));
    }

    #[test]
    fn query_16() {
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
    }

    #[test]
    fn query_17() {
        #[derive(Entity)]
        struct Limitable2 {
            x: u32,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(Limitable2::columns())
                .from(Limitable2::table())
                .order_by(cols!(Limitable2::x ASC))
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
    fn query_19() {
        #[derive(Entity)]
        struct Foo {
            id: u32,
        }
        let ct = QueryBuilder::new().create_table::<Foo>();
        assert!(!ct.get_not_exists());
        let ct2 = QueryBuilder::new().create_table::<Foo>().if_not_exists();
        assert!(ct2.get_not_exists());

        let dt = QueryBuilder::new().drop_table::<Foo>();
        assert!(!dt.get_exists());
        let dt2 = QueryBuilder::new().drop_table::<Foo>().if_exists();
        assert!(dt2.get_exists());
    }

    #[test]
    fn query_20() {
        #[derive(Entity)]
        struct Bar {
            val: u32,
        }
        let rows = vec![Bar { val: 1 }];
        let query = QueryBuilder::new().insert_into::<Bar>().values(&rows);
        assert!(!query.get_update());
    }

    #[test]
    fn query_21() {
        use std::fmt::Write;
        let mut q = DynQuery::default();
        let _ = q.write_str("SELECT 1");
        let s: String = q.into();
        assert_eq!(s, "SELECT 1");
    }

    #[test]
    fn query_22() {
        use tank::RawQuery;
        let rq = RawQuery("SELECT * FROM t".into());
        assert_eq!(format!("{rq}"), "SELECT * FROM t");
    }
}
