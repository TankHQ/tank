#[cfg(test)]
mod tests {
    use quote::ToTokens;
    use rust_decimal::Decimal;
    use std::{
        borrow::Cow,
        collections::{BTreeMap, HashMap},
        ffi::{CString, c_char},
        fmt::Write,
        ptr,
        vec::IntoIter,
    };
    use syn::{Path, parse_str};
    use tank::{
        CollectionType, Context, Dataset, DynQuery, EitherIterator, Entity, Expression,
        FixedDecimal, Fragment, GenericSqlWriter, Interval, Join, JoinType, MapType, QueryResult,
        References, Row, RowLabels, RowOwned, RowsAffected, SelectQuery, Sql, SqlExpressionWriter,
        SqlValueWriter, SqlWriter, StructType, TRUNCATE_LONG_LIMIT, TableRef, Value, as_c_string,
        column_def, consume_while, decimal_from_scaled, error_message_from_ptr, extract_number,
        matches_path, month_to_number, number_to_month, quote_btree_map, quote_cow, quote_option,
        separated_by, truncate_bound, truncate_long, value_to_json, write_escaped,
    };
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
    use uuid::Uuid;

    #[test]
    fn table_ref_full_name() {
        assert_eq!(TableRef::new("users".into()).full_name("."), "users");
        let t = TableRef {
            name: "users".into(),
            schema: "public".into(),
            ..Default::default()
        };
        assert_eq!(t.full_name("."), "public.users");
        assert_eq!(t.full_name("_"), "public_users");
        let t = TableRef {
            name: "users".into(),
            schema: "public".into(),
            alias: "u".into(),
            ..Default::default()
        };
        assert_eq!(t.full_name("."), "u");
    }

    #[test]
    fn table_ref_with_alias() {
        let t = TableRef::new("users".into());
        let aliased = t.with_alias("u".into());
        assert_eq!(aliased.name, "users");
        assert_eq!(aliased.alias, "u");
    }

    #[test]
    fn table_ref_is_empty() {
        assert!(TableRef::default().is_empty());
        assert!(!TableRef::new("t".into()).is_empty());
        let schema_only = TableRef {
            schema: "s".into(),
            ..Default::default()
        };
        assert!(!schema_only.is_empty());
        let alias_only = TableRef {
            alias: "a".into(),
            ..Default::default()
        };
        assert!(!alias_only.is_empty());
    }

    #[test]
    fn table_ref_from_str() {
        let t: TableRef = "my_table".into();
        assert_eq!(t.name, "my_table");
        assert_eq!(t.schema, "");
    }

    #[test]
    fn table_ref_write() {
        let writer = GenericSqlWriter::new();
        let table = TableRef {
            name: "events".into(),
            schema: "analytics".into(),
            alias: "e".into(),
            ..Default::default()
        };

        let mut from_context = Context::fragment(Fragment::SqlSelectFrom);
        let mut rendered = DynQuery::default();
        table.write_table_name(&writer, &mut from_context, &mut rendered);
        assert_eq!(rendered.as_str().as_ref(), r#""analytics"."events" e"#);
        assert_eq!(table.table_ref(), table);
        assert!(!table.qualified_columns());

        let mut join_context = Context::fragment(Fragment::SqlJoin);
        let mut rendered_borrowed = DynQuery::default();
        (&table).write_table_name(&writer, &mut join_context, &mut rendered_borrowed);
        assert_eq!(
            rendered_borrowed.as_str().as_ref(),
            r#""analytics"."events" e"#
        );
        assert_eq!((&&table).table_ref(), table);
        assert!(!table.qualified_columns());

        let dynamic: &dyn Dataset = &table;
        assert!(!dynamic.qualified_columns());
        let mut query = DynQuery::default();
        (&dynamic).write_table_name(
            &writer,
            &mut Context::fragment(Fragment::SqlSelectFrom),
            &mut query,
        );
        assert_eq!(query.as_str().as_ref(), r#""analytics"."events" e"#);
        assert_eq!((&dynamic).table_ref(), table);
    }

    #[test]
    fn table_ref_write_alias_reference() {
        let writer = GenericSqlWriter::new();
        let table = TableRef {
            name: "events".into(),
            schema: "analytics".into(),
            alias: "e".into(),
            ..Default::default()
        };

        let mut rendered = DynQuery::default();
        table.write_table_name(&writer, &mut Context::empty(), &mut rendered);
        assert_eq!(rendered.as_str().as_ref(), "e");
    }

    #[test]
    fn table_ref_to_tokens() {
        let table = TableRef {
            name: "events".into(),
            schema: "analytics".into(),
            alias: "e".into(),
            ..Default::default()
        };

        let tokens = table.to_token_stream().to_string();
        assert!(tokens.contains(":: tank :: TableRef"));
        assert!(tokens.contains("events"));
        assert!(tokens.contains("analytics"));
        assert!(tokens.contains("Borrowed"));
    }

    #[test]
    fn util_quote_cow_owned() {
        let value: Cow<'static, str> = Cow::Owned(String::from("events"));
        let tokens = quote_cow(&value).to_string();
        assert_eq!(tokens, ":: std :: borrow :: Cow :: Owned (\"events\")");
    }

    #[test]
    fn row_accessors() {
        let labels: RowLabels = RowLabels::from(vec!["id".into(), "name".into()]);
        let row = RowOwned::new(
            labels.clone(),
            vec![Value::Int32(Some(1)), Value::Varchar(Some("Alice".into()))],
        );
        assert_eq!(row.labels(), &["id", "name"]);
        assert_eq!(row.values().count(), 2);
        assert_eq!(row.len(), 2);
        assert!(
            row.get_column("id")
                .is_some_and(|v| *v == Value::Int32(Some(1)))
        );
        assert_eq!(row.get_column("missing"), None);
        let pairs: Vec<_> = row.iter().collect();
        assert_eq!(pairs[0].0, "id");
        assert_eq!(*pairs[0].1, Value::Int32(Some(1)));
        let values: Vec<_> = row.into_values().collect();
        assert_eq!(
            values,
            vec![Value::Int32(Some(1)), Value::Varchar(Some("Alice".into()))]
        );

        let result: QueryResult = RowOwned::new(
            RowLabels::from(vec!["id".into()]),
            vec![Value::Int32(Some(1))],
        )
        .into();
        assert!(matches!(result, QueryResult::Row(_)));
    }

    #[test]
    fn rows_affected_extend() {
        let mut a = RowsAffected {
            rows_affected: Some(5),
            last_affected_id: Some(10),
        };
        a.extend([RowsAffected {
            rows_affected: Some(3),
            last_affected_id: Some(20),
        }]);
        assert_eq!(a.rows_affected, Some(8));
        assert_eq!(a.last_affected_id, Some(20));

        let mut a = RowsAffected::default();
        a.extend([RowsAffected::default()]);
        assert_eq!(a.rows_affected, None);
        assert_eq!(a.last_affected_id, None);

        let mut a = RowsAffected::default();
        a.extend([RowsAffected {
            rows_affected: Some(7),
            last_affected_id: None,
        }]);
        assert_eq!(a.rows_affected, Some(7));
    }

    #[test]
    fn rows_affected_to_query_result() {
        let r = RowsAffected {
            rows_affected: Some(1),
            last_affected_id: None,
        };
        let result: QueryResult = r.into();
        assert!(matches!(result, QueryResult::Affected(_)));
    }

    #[test]
    fn dyn_query_operations() {
        let mut q = DynQuery::default();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        q.push_str("SELECT ");
        assert!(!q.is_empty());
        assert_eq!(q.len(), 7);
        q.push('*');
        assert_eq!(q.len(), 8);
        assert_eq!(q.as_str().as_ref(), "SELECT *");

        let mut q = DynQuery::with_capacity(1024);
        assert!(q.is_empty());
        q.push_str("hello");
        assert_eq!(q.as_str().as_ref(), "hello");

        let q = DynQuery::new("SELECT 1".into());
        assert_eq!(q.as_str().as_ref(), "SELECT 1");
        assert_eq!(q.len(), 8);
        assert!(!q.is_empty());

        let mut q = DynQuery::default();
        write!(q, "INSERT INTO {} VALUES ({})", "t", 42).unwrap();
        assert_eq!(q.as_str().as_ref(), "INSERT INTO t VALUES (42)");

        let s: String = DynQuery::new("hello world".into()).into();
        assert_eq!(s, "hello world");

        let mut q = DynQuery::new("initial".into());
        q.buffer().clear();
        assert!(q.is_empty());
        q.buffer().push_str("new content");
        assert_eq!(q.as_str().as_ref(), "new content");
    }

    #[test]
    fn util_value_to_json_scalars() {
        assert_eq!(value_to_json(&Value::Null), Some(serde_json::Value::Null));
        assert_eq!(
            value_to_json(&Value::Boolean(Some(true))),
            Some(serde_json::Value::Bool(true))
        );
        assert_eq!(
            value_to_json(&Value::Int8(Some(42))),
            Some(serde_json::json!(42))
        );
        assert_eq!(
            value_to_json(&Value::Int16(Some(-100))),
            Some(serde_json::json!(-100))
        );
        assert_eq!(
            value_to_json(&Value::Int32(Some(1000))),
            Some(serde_json::json!(1000))
        );
        assert_eq!(
            value_to_json(&Value::Int64(Some(123456789))),
            Some(serde_json::json!(123456789))
        );
        assert_eq!(
            value_to_json(&Value::Int128(Some(999.into()))),
            Some(serde_json::json!(999))
        );
        assert_eq!(
            value_to_json(&Value::UInt8(Some(255))),
            Some(serde_json::json!(255))
        );
        assert_eq!(
            value_to_json(&Value::UInt16(Some(65535))),
            Some(serde_json::json!(65535))
        );
        assert_eq!(
            value_to_json(&Value::UInt32(Some(100))),
            Some(serde_json::json!(100))
        );
        assert_eq!(
            value_to_json(&Value::UInt64(Some(200))),
            Some(serde_json::json!(200))
        );
        assert_eq!(
            value_to_json(&Value::UInt128(Some(300.into()))),
            Some(serde_json::json!(300))
        );
        assert_eq!(
            value_to_json(&Value::Float32(Some(1.5))),
            Some(serde_json::json!(1.5))
        );
        assert_eq!(
            value_to_json(&Value::Float64(Some(2.5))),
            Some(serde_json::json!(2.5))
        );
        assert_eq!(
            value_to_json(&Value::Char(Some('x'))),
            Some(serde_json::json!("x"))
        );
        assert_eq!(
            value_to_json(&Value::Varchar(Some("hello".into()))),
            Some(serde_json::json!("hello"))
        );
        assert_eq!(
            value_to_json(&Value::Unknown(Some("raw".into()))),
            Some(serde_json::json!("raw"))
        );
    }

    #[test]
    fn value_json_conversion() {
        assert_eq!(
            value_to_json(&Value::Boolean(None)),
            Some(serde_json::Value::Null)
        );
        assert_eq!(
            value_to_json(&Value::Int32(None)),
            Some(serde_json::Value::Null)
        );

        let arr = value_to_json(&Value::Blob(Some(vec![0xDE, 0xAD].into()))).unwrap();
        assert_eq!(arr, serde_json::json!([222, 173]));

        assert_eq!(
            value_to_json(&Value::Uuid(Some(uuid::Uuid::nil()))),
            Some(serde_json::json!("00000000-0000-0000-0000-000000000000"))
        );
        let date = Date::from_calendar_date(2025, Month::March, 15).unwrap();
        assert!(value_to_json(&Value::Date(Some(date))).unwrap().is_string());

        let arr = Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(CollectionType {
                element: Value::Int32(None),
                len: 2,
            }),
        );
        assert_eq!(value_to_json(&arr), Some(serde_json::json!([1, 2])));
        let list = Value::List(
            Some(
                vec![
                    Value::Varchar(Some("a".into())),
                    Value::Varchar(Some("b".into())),
                ]
                .into(),
            ),
            Box::new(CollectionType {
                element: Value::Varchar(None),
                len: 0,
            }),
        );
        assert_eq!(value_to_json(&list), Some(serde_json::json!(["a", "b"])));

        let mut map = HashMap::new();
        map.insert(Value::Int32(Some(1)), Value::Int32(Some(42)));
        let v = Value::Map(
            Some(Box::new(map)),
            Box::new(MapType {
                key: Value::Int32(None),
                value: Value::Int32(None),
            }),
        );
        assert_eq!(value_to_json(&v), Some(serde_json::json!({"1": 42})));
        let v = Value::Map(
            Some(Box::new(HashMap::new())),
            Box::new(MapType {
                key: Value::Varchar(None),
                value: Value::Int32(None),
            }),
        );
        assert!(value_to_json(&v).unwrap().is_object());

        let j = serde_json::json!({"nested": true});
        assert_eq!(value_to_json(&Value::Json(Some(j.clone()))), Some(j));

        let s = Value::Struct(
            Some(vec![].into()),
            Box::new(StructType {
                fields: vec![],
                name: TableRef::new("my_type".into()),
            }),
        );
        assert!(value_to_json(&s).unwrap().is_object());

        assert!(value_to_json(&Value::Decimal(Some(Decimal::new(1234, 2)), 0, 0)).is_some());
        assert_eq!(
            value_to_json(&Value::Interval(Some(Interval::from_days(1)))),
            None
        );
        assert_eq!(value_to_json(&Value::Float64(Some(f64::NAN))), None);
    }

    #[test]
    fn util_write_escaped() {
        let mut q = DynQuery::default();
        write_escaped(&mut q, "hello", '\'', "''");
        assert_eq!(q.as_str().as_ref(), "hello");

        let mut q2 = DynQuery::default();
        write_escaped(&mut q2, "it's a test", '\'', "''");
        assert_eq!(q2.as_str().as_ref(), "it''s a test");

        let mut q3 = DynQuery::default();
        write_escaped(&mut q3, "a\"b\"c", '"', "\"\"");
        assert_eq!(q3.as_str().as_ref(), "a\"\"b\"\"c");

        let mut q4 = DynQuery::default();
        write_escaped(&mut q4, "a😀b😀c", '😀', "[]");
        assert_eq!(q4.as_str().as_ref(), "a[]b[]c");
    }

    #[test]
    fn sql_writer_schema_create_drop() {
        #[derive(Entity)]
        #[tank(schema = "analytics", name = "events")]
        struct SchemaEntity {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        writer.write_create_schema(&mut q, SchemaEntity::table(), true);
        assert_eq!(
            q.as_str().as_ref(),
            "CREATE SCHEMA IF NOT EXISTS \"analytics\";"
        );

        let mut q2 = DynQuery::default();
        writer.write_create_schema(&mut q2, SchemaEntity::table(), false);
        assert_eq!(q2.as_str().as_ref(), "CREATE SCHEMA \"analytics\";");

        let mut q3 = DynQuery::default();
        writer.write_drop_schema(&mut q3, SchemaEntity::table(), true);
        assert_eq!(q3.as_str().as_ref(), "DROP SCHEMA IF EXISTS \"analytics\";");
        let mut q4 = DynQuery::default();
        writer.write_drop_schema(&mut q4, SchemaEntity::table(), false);
        assert_eq!(q4.as_str().as_ref(), "DROP SCHEMA \"analytics\";");

        let mut q5 = DynQuery::new("-- c".into());
        writer.write_create_schema(&mut q5, SchemaEntity::table(), false);
        assert!(q5.as_str().starts_with("-- c\nCREATE SCHEMA"));

        let mut out = DynQuery::default();
        writer.write_cast(
            &mut Context::empty(),
            &mut out,
            &tank::Operand::LitInt(1),
            &tank::Operand::Type(Value::Int64(None)),
        );
        assert_eq!(out.as_str(), "CAST(1 AS BIGINT)");
    }

    #[test]
    fn sql_writer_select_without_from() {
        struct NoFrom;
        impl SelectQuery<TableRef> for NoFrom {
            fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
                std::iter::empty::<tank::Operand<'_>>()
            }
            fn get_from(&self) -> &Option<TableRef> {
                &None
            }
            fn get_where(&self) -> &Option<impl Expression> {
                &None::<bool>
            }
            fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
                std::iter::empty::<tank::Operand<'_>>()
            }
            fn get_having(&self) -> &Option<impl Expression> {
                &None::<bool>
            }
            fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
                std::iter::empty::<tank::Operand<'_>>()
            }
            fn get_limit(&self) -> Option<u32> {
                None
            }
            fn build<D: tank::Driver>(&self, _: &D) -> DynQuery {
                DynQuery::default()
            }
            fn build_into<D: tank::Driver>(&self, _: &D, _: &mut DynQuery) {}
        }
        let mut out = DynQuery::default();
        GenericSqlWriter::new().write_select(&mut out, &NoFrom);
        assert_eq!(out.as_str(), "SELECT *;");
    }

    #[test]
    fn sql_writer_select_star() {
        #[derive(Entity)]
        struct StarTable {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();

        let empty_cols: &[&dyn Expression] = &[];
        writer.write_select(&mut q, Sql().select(empty_cols).from(StarTable::table()));
        assert!(q.as_str().contains("SELECT *"));
    }

    #[test]
    fn sql_writer_composite_values() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let mut map = HashMap::new();
        map.insert(Value::Varchar(Some("key1".into())), Value::Int32(Some(100)));
        writer.write_value(
            &mut Default::default(),
            &mut q,
            &Value::Map(
                Some(Box::new(map)),
                Box::new(MapType {
                    key: Value::Varchar(None),
                    value: Value::Int32(None),
                }),
            ),
        );
        assert!(q.as_str().contains("key1"));
        assert!(q.as_str().contains("100"));

        let mut q = DynQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut q,
            &Value::Struct(
                Some(
                    vec![
                        ("name".into(), Value::Varchar(Some("Alice".into()))),
                        ("age".into(), Value::Int32(Some(30))),
                    ]
                    .into(),
                ),
                Box::new(StructType {
                    fields: vec![],
                    name: TableRef::new("person".into()),
                }),
            ),
        );
        assert!(q.as_str().contains("Alice"));
        assert!(q.as_str().contains("30"));

        let mut q = DynQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut q,
            &Value::Json(Some(serde_json::json!({"active": true}))),
        );
        assert!(q.as_str().contains("active"));
    }

    #[test]
    fn sql_writer_write_json_string() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let mut context = Context::fragment(Fragment::Json);
        writer.write_value(
            &mut context,
            &mut q,
            &Value::Varchar(Some(r#"folder\queue"#.into())),
        );
        assert_eq!(q.as_str().as_ref(), r#""folder\\queue""#);
        assert_eq!(
            serde_json::from_str::<String>(q.as_str().as_ref()).unwrap(),
            r#"folder\queue"#
        );

        let date = Date::from_calendar_date(2024, Month::January, 2).unwrap();
        let time = Time::from_hms(1, 2, 3).unwrap();
        let ts = PrimitiveDateTime::new(date, time);
        for (fragment, value, expected) in [
            (Fragment::Json, Value::Null, "null"),
            (Fragment::Json, Value::Date(Some(date)), "\"2024-01-02\""),
            (Fragment::Json, Value::Time(Some(time)), "\"01:02:03.0\""),
            (
                Fragment::Json,
                Value::Timestamp(Some(ts)),
                "\"2024-01-02 01:02:03.0\"",
            ),
            (
                Fragment::Json,
                Value::TimestampWithTimezone(Some(ts.assume_utc())),
                "\"2024-01-02 01:02:03.0\"",
            ),
            (
                Fragment::Json,
                Value::Blob(Some(vec![1, 2].into())),
                "\"\\x01\\x02\"",
            ),
            (
                Fragment::Json,
                Value::Uuid(Some(Uuid::nil())),
                "\"00000000-0000-0000-0000-000000000000\"",
            ),
            (
                Fragment::Json,
                Value::Interval(Some(Interval::from_secs(1))),
                "INTERVAL \"1 SECOND\"",
            ),
            (Fragment::JsonKey, Value::Boolean(Some(true)), "\"true\""),
            (Fragment::JsonKey, Value::Boolean(Some(false)), "\"false\""),
            (Fragment::JsonKey, Value::Int32(Some(3)), "\"3\""),
            (Fragment::JsonKey, Value::Float64(Some(1.5)), "\"1.5\""),
        ] {
            let mut out = DynQuery::default();
            writer.write_value(&mut Context::new(fragment, false), &mut out, &value);
            assert_eq!(out.as_str(), expected);
        }
    }

    #[test]
    fn sql_writer_blob() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut q,
            &Value::Blob(Some(vec![0xCA, 0xFE].into())),
        );
        assert!(q.as_str().contains("CA"));
        assert!(q.as_str().contains("FE"));
    }

    #[test]
    fn sql_writer_write_timestamptz() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let dt = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(10, 30, 0).unwrap(),
            UtcOffset::from_hms(5, 30, 15).unwrap(),
        );
        writer.write_value(
            &mut Default::default(),
            &mut q,
            &Value::TimestampWithTimezone(Some(dt)),
        );
        assert!(q.as_str().contains("2025"));
        assert!(q.as_str().contains("+05:30:15"));
    }

    #[test]
    fn sql_writer_special_floats() {
        let writer = GenericSqlWriter::new();
        for f in [f64::INFINITY, f64::NEG_INFINITY] {
            let mut q = DynQuery::default();
            writer.write_value(&mut Default::default(), &mut q, &Value::Float64(Some(f)));
            let s = q.as_str();
            assert!(s.contains("CAST"));
            assert!(s.contains("inf"));
        }
        for value in [
            Value::Float32(Some(f32::INFINITY)),
            Value::Float32(Some(f32::NAN)),
        ] {
            let mut q = DynQuery::default();
            writer.write_value(&mut Default::default(), &mut q, &value);
            let s = q.as_str();
            assert!(s.contains("CAST"));
            assert!(s.contains("FLOAT"));
            assert!(!s.contains("DOUBLE"));
        }
    }

    #[test]
    fn sql_writer_transaction_statements() {
        let writer = GenericSqlWriter::new();
        let mut q1 = DynQuery::default();
        writer.write_transaction_begin(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;");
        writer.write_transaction_commit(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;\nCOMMIT;");
        writer.write_transaction_rollback(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;\nCOMMIT;\nROLLBACK;");
        let mut q2 = DynQuery::new("SELECT 1".into());
        writer.write_transaction_begin(&mut q2);
        assert_eq!(q2.as_str(), "SELECT 1\nBEGIN;");
    }

    #[test]
    fn sql_writer_insert_empty_entities() {
        #[derive(Entity)]
        struct EmptyInsert {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let entities: Vec<&EmptyInsert> = vec![];
        writer.write_insert(
            &mut q,
            Sql().insert_into(EmptyInsert::table()).values(entities),
        );
        assert!(q.is_empty());
    }

    #[test]
    fn sql_writer_append_to_existing() {
        #[derive(Entity)]
        struct AppendTable {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::new("-- comment".into());
        writer.write_select(
            &mut q,
            Sql()
                .select(AppendTable::columns())
                .from(AppendTable::table()),
        );
        let mut q = DynQuery::new("-- comment".into());
        writer.write_select(
            &mut q,
            Sql()
                .select(AppendTable::columns())
                .from(AppendTable::table()),
        );
        assert!(q.as_str().starts_with("-- comment\nSELECT"));

        let mut q = DynQuery::new("-- c".into());
        writer.write_create_table(&mut q, AppendTable::table(), false);
        assert!(q.as_str().starts_with("-- c\nCREATE TABLE"));
        let mut q = DynQuery::new("-- c".into());
        writer.write_drop_table(&mut q, AppendTable::table(), false);
        assert!(q.as_str().starts_with("-- c\nDROP TABLE"));
        let mut q = DynQuery::new("-- c".into());
        writer.write_delete(&mut q, AppendTable::table(), true);
        assert!(q.as_str().starts_with("-- c\nDELETE FROM"));
        let mut q = DynQuery::new("-- c".into());
        writer.write_insert(
            &mut q,
            Sql()
                .insert_into(AppendTable::table())
                .values(&[AppendTable { id: 1 }]),
        );
        assert!(q.as_str().starts_with("-- c\nINSERT INTO"));
    }

    #[test]
    fn do_nothing_pk_only() {
        #[derive(Entity)]
        struct PkOnly {
            #[tank(primary_key)]
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        writer.write_insert(
            &mut q,
            Sql()
                .insert_into(PkOnly::table())
                .values(&[PkOnly { id: 1 }])
                .update(),
        );
        assert!(q.as_str().contains("DO NOTHING"));

        #[derive(Entity)]
        struct NoPk {
            value: i32,
        }
        let mut q = DynQuery::default();
        writer.write_insert(
            &mut q,
            Sql()
                .insert_into(NoPk::table())
                .values(&[NoPk { value: 1 }])
                .update(),
        );
        assert!(!q.as_str().contains("ON CONFLICT"));
    }

    #[test]
    fn util_either_iterator() {
        let left: EitherIterator<IntoIter<i32>, IntoIter<i32>> =
            EitherIterator::Left(vec![1, 2, 3].into_iter());
        assert_eq!(left.collect::<Vec<_>>(), vec![1, 2, 3]);

        let right: EitherIterator<IntoIter<i32>, IntoIter<i32>> =
            EitherIterator::Right(vec![4, 5].into_iter());
        assert_eq!(right.collect::<Vec<_>>(), vec![4, 5]);
    }

    #[test]
    fn util_consume_while() {
        let mut input = "12345abc";
        let digits = consume_while(&mut input, |c| c.is_ascii_digit());
        assert_eq!(digits, "12345");
        assert_eq!(input, "abc");

        let mut input2 = "abc";
        let empty = consume_while(&mut input2, |c| c.is_ascii_digit());
        assert_eq!(empty, "");
        assert_eq!(input2, "abc");

        let mut input3 = "ééé123";
        let accented = consume_while(&mut input3, |c| !c.is_ascii_digit());
        assert_eq!(accented, "ééé");
        assert_eq!(input3, "123");
    }

    #[test]
    fn util_extract_number() {
        let mut input = "123abc";
        let num = extract_number::<false>(&mut input);
        assert_eq!(num, "123");
        assert_eq!(input, "abc");

        let mut input2 = "-42rest";
        let num2 = extract_number::<true>(&mut input2);
        assert_eq!(num2, "-42");
        assert_eq!(input2, "rest");

        let mut input3 = "+99x";
        let num3 = extract_number::<true>(&mut input3);
        assert_eq!(num3, "+99");
        assert_eq!(input3, "x");

        let mut input4 = "abc";
        let num4 = extract_number::<false>(&mut input4);
        assert_eq!(num4, "");
    }

    #[test]
    fn util_as_c_string() {
        let cs = as_c_string("hello");
        assert_eq!(cs.to_str().unwrap(), "hello");

        let cs2 = as_c_string(vec![104, 0, 105]);
        assert_eq!(cs2.to_str().unwrap(), "h?i");
    }

    #[test]
    fn util_separated_by() {
        let mut out = DynQuery::default();
        separated_by(
            &mut out,
            [1, 2, 3],
            |o, v| {
                let _ = write!(o, "{v}");
            },
            ", ",
        );
        assert_eq!(out.as_str(), "1, 2, 3");

        let mut out2 = DynQuery::default();
        separated_by(
            &mut out2,
            [42],
            |o, v| {
                let _ = write!(o, "{v}");
            },
            ", ",
        );
        assert_eq!(out2.as_str(), "42");
    }

    #[test]
    fn util_column_def() {
        #[derive(Entity)]
        struct CdTable {
            my_col: i32,
        }
        let table = CdTable::table();
        let found = column_def("my_col", table);
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "my_col");
        let not_found = column_def("nonexistent", table);
        assert!(not_found.is_none());
    }

    #[test]
    fn context_navigation() {
        let c = Context::empty();
        assert_eq!(c.fragment, Fragment::None);
        assert!(!c.qualify_columns);
        assert!(!c.quote_identifiers);
        let c2 = Context::fragment(Fragment::SqlSelect);
        assert_eq!(c2.fragment, Fragment::SqlSelect);
        assert!(!c2.qualify_columns);
        assert!(c2.quote_identifiers);
        assert!(Context::qualify(true).qualify_columns);
        assert!(Context::qualify_with("my_table".into()).qualify_columns);

        let mut ctx = Context::new(Fragment::SqlSelect, false);
        ctx.counter = 5;
        {
            let mut updater = ctx.switch_fragment(Fragment::SqlSelectWhere);
            assert_eq!(updater.current.fragment, Fragment::SqlSelectWhere);
            updater.current.counter = 10;
        }
        assert_eq!(ctx.counter, 10);
        assert_eq!(ctx.fragment, Fragment::SqlSelect);

        assert!(
            ctx.switch_table(TableRef::new("my_tbl".into()))
                .current
                .qualify_columns
        );
        assert!(
            !ctx.switch_table(TableRef::new("".into()))
                .current
                .qualify_columns
        );

        ctx.update_from(&Context {
            counter: 42,
            ..Context::new(Fragment::SqlSelect, false)
        });
        assert_eq!(ctx.counter, 42);
    }

    #[test]
    fn join_qualified_columns() {
        #[derive(Entity)]
        #[tank(schema = "sch")]
        struct Tbl1 {
            _x: i32,
        }
        #[derive(Entity)]
        struct Tbl2 {
            _y: i32,
        }
        let join = Join::<_, _, ()> {
            join: JoinType::Inner,
            lhs: Tbl1::table().clone(),
            rhs: Tbl2::table().clone(),
            on: None,
        };

        let tr = join.table_ref();
        assert!(tr.name.is_empty());
    }

    #[test]
    fn join_table_ref_same_schema() {
        #[derive(Entity)]
        #[tank(schema = "common")]
        struct Left1 {
            _a: i32,
        }
        #[derive(Entity)]
        #[tank(schema = "common")]
        struct Right1 {
            _b: i32,
        }
        let join = Join::<_, _, ()> {
            join: JoinType::Left,
            lhs: Left1::table().clone(),
            rhs: Right1::table().clone(),
            on: None,
        };
        let tr = join.table_ref();

        assert_eq!(tr.schema.as_ref(), "common");
    }

    #[test]
    fn relations_fixed_decimal() {
        let fd: FixedDecimal<10, 2> = Decimal::from(42).into();
        let back: Decimal = fd.into();
        assert_eq!(back, Decimal::from(42));
    }

    #[test]
    fn relations_references() {
        #[derive(Entity)]
        struct RefTarget {
            _id: i32,
        }
        let refs = References::<RefTarget>::new(Box::new([]));
        assert_eq!(refs.table_ref().name.as_ref(), "ref_target");
        assert!(refs.columns().is_empty());
    }

    // Tests kept from the branch that were not present on master
    #[test]
    fn util_quote_helpers() {
        let map = BTreeMap::from([("a", "b"), ("c", "d")]);
        let tokens = quote_btree_map(&map).to_string();
        assert!(tokens.contains(":: std :: collections :: BTreeMap :: from"));
        assert!(tokens.contains("\"a\""));

        assert_eq!(quote_option::<&str>(&None).to_string(), "None");
        assert_eq!(quote_option(&Some("v")).to_string(), "Some (\"v\")");
    }

    #[test]
    fn value_to_json_unrepresentable() {
        let mut map = HashMap::new();
        map.insert(
            Value::Int32(Some(1)),
            Value::Interval(Some(Interval::from_days(1))),
        );
        assert_eq!(
            value_to_json(&Value::Map(
                Some(map.into()),
                Box::new(MapType {
                    key: Value::Int32(None),
                    value: Value::Interval(None)
                })
            )),
            None
        );

        let mut map = HashMap::new();
        map.insert(
            Value::Interval(Some(Interval::from_days(1))),
            Value::Int32(Some(1)),
        );
        assert_eq!(
            value_to_json(&Value::Map(
                Some(map.into()),
                Box::new(MapType {
                    key: Value::Interval(None),
                    value: Value::Int32(None)
                })
            )),
            None
        );

        assert_eq!(
            value_to_json(&Value::Struct(
                Some(vec![("k".into(), Value::Interval(Some(Interval::from_days(1))))].into()),
                Box::new(StructType {
                    fields: vec![],
                    name: TableRef::new("t".into())
                })
            )),
            None
        );
        assert_eq!(
            value_to_json(&Value::Decimal(None, 0, 0)),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn util_decimal_from_scaled() {
        assert_eq!(decimal_from_scaled(1234, 2).unwrap(), Decimal::new(1234, 2));
        assert_eq!(
            decimal_from_scaled(200_i128, 29).unwrap(),
            Decimal::new(20, 28)
        );
        assert!(decimal_from_scaled(i128::MAX, 0).is_err());
    }

    #[test]
    fn util_month_conversions() {
        for (number, month) in [
            (1, Month::January),
            (2, Month::February),
            (3, Month::March),
            (4, Month::April),
            (5, Month::May),
            (6, Month::June),
            (7, Month::July),
            (8, Month::August),
            (9, Month::September),
            (10, Month::October),
            (11, Month::November),
            (12, Month::December),
        ] {
            assert_eq!(month_to_number!(month), number);
            assert_eq!(number_to_month!(number, Month::January), month);
        }
        assert_eq!(number_to_month!(13, Month::January), Month::January);
    }

    #[test]
    fn util_truncate_long_multibyte() {
        assert_eq!(format!("{}", truncate_long!("SELECT 1")), "SELECT 1");

        let long_ascii = "X".repeat(TRUNCATE_LONG_LIMIT + 100);
        let truncated = format!("{}", truncate_long!(long_ascii));
        assert!(truncated.ends_with("...\n"));
        assert_eq!(
            truncated.trim_end_matches("...\n").trim().len(),
            TRUNCATE_LONG_LIMIT
        );

        let long_utf8 = "€".repeat(TRUNCATE_LONG_LIMIT);
        let truncated = format!("{}", truncate_long!(long_utf8));
        assert!(truncated.ends_with("...\n"));

        let truncated = format!("{}", truncate_long!(long_utf8.clone(), true));
        assert!(truncated.ends_with("...\n"));
    }

    #[test]
    fn row_get_column_mismatched_lengths() {
        let labels: RowLabels = RowLabels::from(vec!["a".to_string(), "b".to_string()]);
        let values = vec![Value::Int32(Some(1))];
        let row = RowOwned::new(labels, values);
        assert_eq!(
            row.get_column("a").map(Cow::into_owned),
            Some(Value::Int32(Some(1)))
        );
        assert_eq!(row.get_column("b"), None);
    }

    #[test]
    fn truncate_bound_edges() {
        assert_eq!(truncate_bound("abc", 10), 3);
        assert_eq!(truncate_bound("abcdef", 3), 3);
        assert_eq!(truncate_bound("aé", 2), 1);
        assert_eq!(truncate_bound("aé", 3), 3);
        assert_eq!(truncate_bound("", 0), 0);
    }

    #[test]
    fn error_ptr_messages() {
        let text = CString::new("boom").unwrap();
        let raw: *const c_char = text.as_ptr();
        assert_eq!(error_message_from_ptr(raw), "boom");

        let null: *const c_char = ptr::null();
        assert!(error_message_from_ptr(null).contains("Unknown error"));
    }

    #[test]
    fn matches_path_matches_trailing_segments() {
        let path: Path = parse_str("std::num::NonZeroI32").unwrap();
        assert!(matches_path(&path, &["std", "num", "NonZeroI32"]));
        assert!(matches_path(&path, &["num", "NonZeroI32"]));
        assert!(!matches_path(&path, &["std", "num", "NonZeroI64"]));
    }
}
