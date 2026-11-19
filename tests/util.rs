#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::{DRIVER, GenericDriver};
    use quote::ToTokens;
    use rust_decimal::Decimal;
    use std::{
        borrow::Cow,
        collections::{BTreeMap, HashMap},
        ffi::{CString, c_char},
        fmt::Write,
        iter, ptr,
        vec::IntoIter,
    };
    use syn::{Path, parse_str};
    use tank::{
        AnyQuery, CollectionType, Connection, Context, Dataset, Driver, EitherIterator, Entity,
        Expression, FixedDecimal, Fragment, GenericSqlWriter, Interval, Join, JoinType, MapType,
        Operand, QueryResult, References, Row, RowLabels, RowOwned, RowsAffected, SelectQuery, Sql,
        SqlExpressionWriter, SqlValueWriter, SqlWriter, StructType, TRUNCATE_LONG_LIMIT, TableRef,
        Value, as_c_string, column_def, consume_while, decimal_from_scaled, error_message_from_ptr,
        extract_number, matches_path, month_to_number, number_to_month, quote_btree_map, quote_cow,
        quote_option, separated_by, truncate_bound, truncate_long, write_escaped,
    };
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
    use uuid::Uuid;

    #[test]
    fn table_ref_name() {
        assert_eq!(TableRef::new("users".into()).full_name("."), "users");
        let t = TableRef::from(("public", "users"));
        assert_eq!(t.full_name("."), "public.users");
        assert_eq!(t.full_name("_"), "public_users");
        let t = TableRef::from(("public", "users")).with_alias("u".into());
        assert_eq!(t.full_name("."), "u");
    }

    #[test]
    fn table_ref_alias() {
        let t = TableRef::new("users".into());
        let aliased = t.with_alias("u".into());
        assert_eq!(aliased.name, "users");
        assert_eq!(aliased.alias, "u");
    }

    #[test]
    fn table_ref_empty() {
        assert!(TableRef::default().is_empty());
        assert!(!TableRef::new("t".into()).is_empty());
        let mut schema_only = TableRef::default();
        schema_only.schema = "s".into();
        assert!(!schema_only.is_empty());
        let alias_only = TableRef::default().with_alias("a".into());
        assert!(!alias_only.is_empty());
    }

    #[test]
    fn table_ref_parse() {
        let t: TableRef = "my_table".into();
        assert_eq!(t.name, "my_table");
        assert_eq!(t.schema, "");
    }

    #[test]
    fn table_ref_write() {
        let writer = GenericSqlWriter::new();
        let table = TableRef::from(("analytics", "events")).with_alias("e".into());

        let mut from_context = Context::fragment(Fragment::SqlSelectFrom);
        let mut rendered = AnyQuery::default();
        table.write_table_name(&writer, &mut from_context, &mut rendered);
        assert_eq!(rendered.as_str().as_ref(), r#""analytics"."events" e"#);
        assert_eq!(table.table_ref(), table);
        assert!(!table.qualified_columns());

        let mut join_context = Context::fragment(Fragment::SqlJoin);
        let mut rendered_borrowed = AnyQuery::default();
        (&table).write_table_name(&writer, &mut join_context, &mut rendered_borrowed);
        assert_eq!(
            rendered_borrowed.as_str().as_ref(),
            r#""analytics"."events" e"#
        );
        assert_eq!((&&table).table_ref(), table);
        assert!(!table.qualified_columns());

        let dynamic: &dyn Dataset = &table;
        assert!(!dynamic.qualified_columns());
        let mut query = AnyQuery::default();
        (&dynamic).write_table_name(
            &writer,
            &mut Context::fragment(Fragment::SqlSelectFrom),
            &mut query,
        );
        assert_eq!(query.as_str().as_ref(), r#""analytics"."events" e"#);
        assert_eq!((&dynamic).table_ref(), table);
    }

    #[test]
    fn alias_reference_write() {
        let writer = GenericSqlWriter::new();
        let table = TableRef::from(("analytics", "events")).with_alias("e".into());

        let mut rendered = AnyQuery::default();
        table.write_table_name(&writer, &mut Context::empty(), &mut rendered);
        assert_eq!(rendered.as_str().as_ref(), "e");
    }

    #[test]
    fn table_ref_tokens() {
        let table = TableRef::from(("analytics", "events")).with_alias("e".into());

        let tokens = table.to_token_stream().to_string();
        assert!(tokens.contains(":: tank :: TableRef"));
        assert!(tokens.contains("events"));
        assert!(tokens.contains("analytics"));
        assert!(tokens.contains("Borrowed"));
    }

    #[test]
    fn quote_cow_owned() {
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
    fn rows_affected_result() {
        let r = RowsAffected {
            rows_affected: Some(1),
            last_affected_id: None,
        };
        let result: QueryResult = r.into();
        assert!(matches!(result, QueryResult::Affected(_)));
    }

    #[test]
    fn any_query_operations() {
        let mut query = AnyQuery::default();
        assert!(query.is_empty());
        assert_eq!(query.len(), 0);
        query.push_str("SELECT ");
        assert!(!query.is_empty());
        assert_eq!(query.len(), 7);
        query.push('*');
        assert_eq!(query.len(), 8);
        assert_eq!(query.as_str().as_ref(), "SELECT *");

        let mut query = AnyQuery::with_capacity(1024);
        assert!(query.is_empty());
        query.push_str("hello");
        assert_eq!(query.as_str().as_ref(), "hello");

        let query = AnyQuery::new("SELECT 1".into());
        assert_eq!(query.as_str().as_ref(), "SELECT 1");
        assert_eq!(query.len(), 8);
        assert!(!query.is_empty());

        let mut query = AnyQuery::default();
        write!(query, "INSERT INTO {} VALUES ({})", "t", 42).unwrap();
        assert_eq!(query.as_str().as_ref(), "INSERT INTO t VALUES (42)");

        let string: String = AnyQuery::new("hello world".into()).into();
        assert_eq!(string, "hello world");

        let mut query = AnyQuery::new("initial".into());
        query.buffer().clear();
        assert!(query.is_empty());
        query.buffer().push_str("new content");
        assert_eq!(query.as_str().as_ref(), "new content");
    }

    #[test]
    fn value_json_scalars() {
        assert_eq!(
            serde_json::Value::try_from(Value::Null).ok(),
            Some(serde_json::Value::Null)
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Boolean(Some(true))).ok(),
            Some(serde_json::Value::Bool(true))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int8(Some(42))).ok(),
            Some(serde_json::json!(42))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int16(Some(-100))).ok(),
            Some(serde_json::json!(-100))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int32(Some(1000))).ok(),
            Some(serde_json::json!(1000))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int64(Some(123456789))).ok(),
            Some(serde_json::json!(123456789))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int128(Some(999.into()))).ok(),
            Some(serde_json::json!(999))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::UInt8(Some(255))).ok(),
            Some(serde_json::json!(255))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::UInt16(Some(65535))).ok(),
            Some(serde_json::json!(65535))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::UInt32(Some(100))).ok(),
            Some(serde_json::json!(100))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::UInt64(Some(200))).ok(),
            Some(serde_json::json!(200))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::UInt128(Some(300.into()))).ok(),
            Some(serde_json::json!(300))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Float32(Some(1.5))).ok(),
            Some(serde_json::json!(1.5))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Float64(Some(2.5))).ok(),
            Some(serde_json::json!(2.5))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Char(Some('x'))).ok(),
            Some(serde_json::json!("x"))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Varchar(Some("hello".into()))).ok(),
            Some(serde_json::json!("hello"))
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Unknown(Some("raw".into()))).ok(),
            Some(serde_json::json!("raw"))
        );
    }

    #[test]
    fn value_json_conversion() {
        assert_eq!(
            serde_json::Value::try_from(Value::Boolean(None)).ok(),
            Some(serde_json::Value::Null)
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Int32(None)).ok(),
            Some(serde_json::Value::Null)
        );

        let arr = serde_json::Value::try_from(Value::Blob(Some(vec![0xDE, 0xAD].into()))).unwrap();
        assert_eq!(arr, serde_json::json!([222, 173]));

        assert_eq!(
            serde_json::Value::try_from(Value::Uuid(Some(uuid::Uuid::nil()))).ok(),
            Some(serde_json::json!("00000000-0000-0000-0000-000000000000"))
        );
        let date = Date::from_calendar_date(2025, Month::March, 15).unwrap();
        assert!(
            serde_json::Value::try_from(Value::Date(Some(date)))
                .unwrap()
                .is_string()
        );

        let arr = Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(CollectionType {
                element: Value::Int32(None),
                len: 2,
            }),
        );
        assert_eq!(
            serde_json::Value::try_from(arr).ok(),
            Some(serde_json::json!([1, 2]))
        );
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
        assert_eq!(
            serde_json::Value::try_from(list).ok(),
            Some(serde_json::json!(["a", "b"]))
        );

        let mut map = HashMap::new();
        map.insert(Value::Int32(Some(1)), Value::Int32(Some(42)));
        let v = Value::Map(
            Some(Box::new(map)),
            Box::new(MapType {
                key: Value::Int32(None),
                value: Value::Int32(None),
            }),
        );
        assert_eq!(
            serde_json::Value::try_from(v).ok(),
            Some(serde_json::json!({"1": 42}))
        );
        let v = Value::Map(
            Some(Box::new(HashMap::new())),
            Box::new(MapType {
                key: Value::Varchar(None),
                value: Value::Int32(None),
            }),
        );
        assert!(serde_json::Value::try_from(v).unwrap().is_object());

        let j = serde_json::json!({"nested": true});
        assert_eq!(
            serde_json::Value::try_from(Value::Json(Some(j.clone()))).ok(),
            Some(j)
        );

        let s = Value::Struct(
            Some(vec![].into()),
            Box::new(StructType {
                fields: vec![],
                name: TableRef::new("my_type".into()),
            }),
        );
        assert!(serde_json::Value::try_from(s).unwrap().is_object());

        assert!(
            serde_json::Value::try_from(Value::Decimal(Some(Decimal::new(1234, 2)), 0, 0)).is_ok()
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Interval(Some(Interval::from_days(1)))).ok(),
            None
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Float64(Some(f64::NAN))).ok(),
            None
        );
    }

    #[test]
    fn util_write_escaped() {
        let mut v = AnyQuery::default();
        write_escaped(&mut v, "hello", '\'', "''");
        assert_eq!(v.as_str().as_ref(), "hello");

        let mut v = AnyQuery::default();
        write_escaped(&mut v, "it's a test", '\'', "''");
        assert_eq!(v.as_str().as_ref(), "it''s a test");

        let mut v = AnyQuery::default();
        write_escaped(&mut v, "a\"b\"c", '"', "\"\"");
        assert_eq!(v.as_str().as_ref(), "a\"\"b\"\"c");

        let mut v = AnyQuery::default();
        write_escaped(&mut v, "a😀b😀c", '😀', "[]");
        assert_eq!(v.as_str().as_ref(), "a[]b[]c");
    }

    #[test]
    fn schema_create_drop() {
        #[derive(Entity)]
        #[tank(schema = "analytics", name = "events")]
        struct SchemaEntity {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = AnyQuery::default();
        writer.write_create_schema(&mut q, SchemaEntity::table(), true);
        assert_eq!(
            q.as_str().as_ref(),
            "CREATE SCHEMA IF NOT EXISTS \"analytics\";"
        );

        let mut q2 = AnyQuery::default();
        writer.write_create_schema(&mut q2, SchemaEntity::table(), false);
        assert_eq!(q2.as_str().as_ref(), "CREATE SCHEMA \"analytics\";");

        let mut q3 = AnyQuery::default();
        writer.write_drop_schema(&mut q3, SchemaEntity::table(), true);
        assert_eq!(q3.as_str().as_ref(), "DROP SCHEMA IF EXISTS \"analytics\";");
        let mut q4 = AnyQuery::default();
        writer.write_drop_schema(&mut q4, SchemaEntity::table(), false);
        assert_eq!(q4.as_str().as_ref(), "DROP SCHEMA \"analytics\";");

        let mut q5 = AnyQuery::new("-- c".into());
        writer.write_create_schema(&mut q5, SchemaEntity::table(), false);
        assert!(q5.as_str().starts_with("-- c\nCREATE SCHEMA"));

        let mut out = AnyQuery::default();
        writer.write_cast(
            &mut Context::empty(),
            &mut out,
            &Operand::LitInt(1),
            &Operand::Type(Value::Int64(None)),
        );
        assert_eq!(out.as_str(), "CAST(1 AS BIGINT)");
    }

    #[test]
    fn select_without_from() {
        struct NoFrom;
        impl SelectQuery<TableRef> for NoFrom {
            fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
                iter::empty::<Operand<'_>>()
            }
            fn get_from(&self) -> &Option<TableRef> {
                &None
            }
            fn get_where(&self) -> &Option<impl Expression> {
                &None::<bool>
            }
            fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
                iter::empty::<Operand<'_>>()
            }
            fn get_having(&self) -> &Option<impl Expression> {
                &None::<bool>
            }
            fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
                iter::empty::<Operand<'_>>()
            }
            fn get_limit(&self) -> Option<u32> {
                None
            }
            fn get_offset(&self) -> Option<u32> {
                None
            }
            fn is_distinct(&self) -> bool {
                false
            }
        }
        let mut out = AnyQuery::default();
        GenericSqlWriter::new().write_select(&mut out, &NoFrom);
        assert_eq!(out.as_str(), "SELECT *;");
    }

    #[test]
    fn writer_select_star() {
        #[derive(Entity)]
        struct StarTable {
            id: i32,
        }
        let empty_cols: &[&dyn Expression] = &[];
        let query = Sql()
            .select(empty_cols)
            .from(StarTable::table())
            .build(&DRIVER);
        assert!(query.as_str().contains("SELECT *"));
    }

    #[test]
    fn writer_composite_values() {
        let writer = GenericSqlWriter::new();
        let mut value = AnyQuery::default();
        let mut map = HashMap::new();
        map.insert(Value::Varchar(Some("key1".into())), Value::Int32(Some(100)));
        writer.write_value(
            &mut Default::default(),
            &mut value,
            &Value::Map(
                Some(Box::new(map)),
                Box::new(MapType {
                    key: Value::Varchar(None),
                    value: Value::Int32(None),
                }),
            ),
        );
        assert!(value.as_str().contains("key1"));
        assert!(value.as_str().contains("100"));

        let mut value = AnyQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut value,
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
        assert!(value.as_str().contains("Alice"));
        assert!(value.as_str().contains("30"));

        let mut value = AnyQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut value,
            &Value::Json(Some(serde_json::json!({"active": true}))),
        );
        assert!(value.as_str().contains("active"));
    }

    #[test]
    fn write_json_string() {
        let writer = GenericSqlWriter::new();
        let mut value = AnyQuery::default();
        let mut context = Context::fragment(Fragment::Json);
        writer.write_value(
            &mut context,
            &mut value,
            &Value::Varchar(Some(r#"folder\queue"#.into())),
        );
        assert_eq!(value.as_str().as_ref(), r#""folder\\queue""#);
        assert_eq!(
            serde_json::from_str::<String>(value.as_str().as_ref()).unwrap(),
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
            let mut out = AnyQuery::default();
            writer.write_value(&mut Context::new(fragment, false), &mut out, &value);
            assert_eq!(out.as_str(), expected);
        }
    }

    #[test]
    fn sql_writer_blob() {
        let writer = GenericSqlWriter::new();
        let mut value = AnyQuery::default();
        writer.write_value(
            &mut Default::default(),
            &mut value,
            &Value::Blob(Some(vec![0xCA, 0xFE].into())),
        );
        assert!(value.as_str().contains("CA"));
        assert!(value.as_str().contains("FE"));
    }

    #[test]
    fn write_timestamptz_value() {
        let writer = GenericSqlWriter::new();
        let mut value = AnyQuery::default();
        let dt = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(10, 30, 0).unwrap(),
            UtcOffset::from_hms(5, 30, 15).unwrap(),
        );
        writer.write_value(
            &mut Default::default(),
            &mut value,
            &Value::TimestampWithTimezone(Some(dt)),
        );
        assert!(value.as_str().contains("2025"));
        assert!(value.as_str().contains("+05:30:15"));
    }

    #[test]
    fn sql_writer_floats() {
        let writer = GenericSqlWriter::new();
        for f in [f64::INFINITY, f64::NEG_INFINITY] {
            let mut value = AnyQuery::default();
            writer.write_value(
                &mut Default::default(),
                &mut value,
                &Value::Float64(Some(f)),
            );
            let s = value.as_str();
            assert!(s.contains("CAST"));
            assert!(s.contains("inf"));
        }
        for value in [
            Value::Float32(Some(f32::INFINITY)),
            Value::Float32(Some(f32::NAN)),
        ] {
            let mut query = AnyQuery::default();
            writer.write_value(&mut Default::default(), &mut query, &value);
            let s = query.as_str();
            assert!(s.contains("CAST"));
            assert!(s.contains("FLOAT"));
            assert!(!s.contains("DOUBLE"));
        }
    }

    #[test]
    fn sql_writer_transactions() {
        let writer = GenericSqlWriter::new();
        let mut q1 = AnyQuery::default();
        writer.write_transaction_begin(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;");
        writer.write_transaction_commit(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;\nCOMMIT;");
        writer.write_transaction_rollback(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;\nCOMMIT;\nROLLBACK;");
        let mut q2 = AnyQuery::new("SELECT 1".into());
        writer.write_transaction_begin(&mut q2);
        assert_eq!(q2.as_str(), "SELECT 1\nBEGIN;");
    }

    #[test]
    fn empty_entities_insert() {
        #[derive(Entity)]
        struct EmptyInsert {
            id: i32,
        }
        let mut q = AnyQuery::default();
        let entities: Vec<&EmptyInsert> = vec![];
        Sql()
            .insert_into(EmptyInsert::table())
            .values(entities)
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.is_empty());
    }

    #[test]
    fn sql_writer_append() {
        #[derive(Entity)]
        struct AppendTable {
            id: i32,
        }
        let mut q = AnyQuery::new("-- comment".into());
        Sql()
            .select(AppendTable::columns())
            .from(AppendTable::table())
            .write_into(&DRIVER.sql_writer(), &mut q);
        let mut q = AnyQuery::new("-- comment".into());
        Sql()
            .select(AppendTable::columns())
            .from(AppendTable::table())
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.as_str().starts_with("-- comment\nSELECT"));

        let mut q = AnyQuery::new("-- c".into());
        Sql()
            .create_table(AppendTable::table())
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.as_str().starts_with("-- c\nCREATE TABLE"));
        let mut q = AnyQuery::new("-- c".into());
        Sql()
            .drop_table(AppendTable::table())
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.as_str().starts_with("-- c\nDROP TABLE"));
        let mut q = AnyQuery::new("-- c".into());
        Sql()
            .delete_from(AppendTable::table())
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.as_str().starts_with("-- c\nDELETE FROM"));
        let mut q = AnyQuery::new("-- c".into());
        Sql()
            .insert_into(AppendTable::table())
            .values(&[AppendTable { id: 1 }])
            .write_into(&DRIVER.sql_writer(), &mut q);
        assert!(q.as_str().starts_with("-- c\nINSERT INTO"));
    }

    #[test]
    fn nothing_pk_only() {
        #[derive(Entity)]
        struct PkOnly {
            #[tank(primary_key)]
            id: i32,
        }
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(PkOnly::table())
            .values(&[PkOnly { id: 1 }])
            .update()
            .write_into(&DRIVER.sql_writer(), &mut query);
        assert!(query.as_str().contains("DO NOTHING"));

        #[derive(Entity)]
        struct NoPk {
            value: i32,
        }
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(NoPk::table())
            .values(&[NoPk { value: 1 }])
            .update()
            .write_into(&DRIVER.sql_writer(), &mut query);
        assert!(!query.as_str().contains("ON CONFLICT"));
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
    fn c_string_conversion() {
        let cs = as_c_string("hello");
        assert_eq!(cs.to_str().unwrap(), "hello");

        let cs2 = as_c_string(vec![104, 0, 105]);
        assert_eq!(cs2.to_str().unwrap(), "h?i");
    }

    #[test]
    fn util_separated_by() {
        let mut out = AnyQuery::default();
        separated_by(
            &mut out,
            [1, 2, 3],
            |o, v| {
                let _ = write!(o, "{v}");
            },
            ", ",
        );
        assert_eq!(out.as_str(), "1, 2, 3");

        let mut out2 = AnyQuery::default();
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
    fn join_same_schema() {
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
    fn json_unrepresentable() {
        let mut map = HashMap::new();
        map.insert(
            Value::Int32(Some(1)),
            Value::Interval(Some(Interval::from_days(1))),
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Map(
                Some(map.into()),
                Box::new(MapType {
                    key: Value::Int32(None),
                    value: Value::Interval(None)
                })
            ))
            .ok(),
            None
        );

        let mut map = HashMap::new();
        map.insert(
            Value::Interval(Some(Interval::from_days(1))),
            Value::Int32(Some(1)),
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Map(
                Some(map.into()),
                Box::new(MapType {
                    key: Value::Interval(None),
                    value: Value::Int32(None)
                })
            ))
            .ok(),
            None
        );

        assert_eq!(
            serde_json::Value::try_from(Value::Struct(
                Some(vec![("k".into(), Value::Interval(Some(Interval::from_days(1))))].into()),
                Box::new(StructType {
                    fields: vec![],
                    name: TableRef::new("t".into())
                })
            ))
            .ok(),
            None
        );
        assert_eq!(
            serde_json::Value::try_from(Value::Decimal(None, 0, 0)).ok(),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn decimal_scaled_conversion() {
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
    fn truncate_multibyte() {
        assert_eq!(format!("{}", truncate_long("SELECT 1")), "SELECT 1");

        let long_ascii = "X".repeat(TRUNCATE_LONG_LIMIT + 100);
        let truncated = format!("{}", truncate_long(&long_ascii));
        assert!(truncated.ends_with("...\n"));
        assert_eq!(
            truncated.trim_end_matches("...\n").trim().len(),
            TRUNCATE_LONG_LIMIT
        );

        let long_utf8 = "€".repeat(TRUNCATE_LONG_LIMIT);
        let truncated = format!("{}", truncate_long(&long_utf8));
        assert!(truncated.ends_with("...\n"));

        // The input expression is evaluated once.
        fn input<'a>(calls: &mut usize, text: &'a str) -> &'a str {
            *calls += 1;
            text
        }
        let mut calls = 0;
        let truncated = format!("{}", truncate_long(input(&mut calls, &long_utf8)));
        assert!(truncated.ends_with("...\n"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn column_length_mismatch() {
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
        assert_eq!(error_message_from_ptr(&raw), "boom");

        let null: *const c_char = ptr::null();
        assert!(error_message_from_ptr(&null).contains("Unknown error"));
    }

    #[test]
    fn sanitize_url_schemes() {
        let error = <GenericDriver as Connection>::sanitize_url(&DRIVER, "bogus://host".into())
            .expect_err("The scheme is not accepted");
        assert!(
            format!("{error:#}").contains(r#"Must start with "generic://""#),
            "{error:#}"
        );
    }

    #[test]
    fn matches_path_segments() {
        let path: Path = parse_str("std::num::NonZeroI32").unwrap();
        assert!(matches_path(&path, &["std", "num", "NonZeroI32"]));
        assert!(matches_path(&path, &["num", "NonZeroI32"]));
        assert!(!matches_path(&path, &["std", "num", "NonZeroI64"]));
    }
}
