#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tank::{
        DynQuery, Entity, GenericSqlWriter, QueryResult, Row, RowsAffected, SqlWriter, TableRef,
        Value, value_to_json,
    };

    #[test]
    fn table_ref_full_name_no_schema() {
        let t = TableRef::new("users".into());
        assert_eq!(t.full_name("."), "users");
    }

    #[test]
    fn table_ref_full_name_with_schema() {
        let t = TableRef {
            name: "users".into(),
            schema: "public".into(),
            ..Default::default()
        };
        assert_eq!(t.full_name("."), "public.users");
        assert_eq!(t.full_name("_"), "public_users");
    }

    #[test]
    fn table_ref_full_name_with_alias() {
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
    fn row_new_and_accessors() {
        let labels: Arc<[String]> = Arc::from(vec!["id".into(), "name".into()]);
        let values: Box<[Value]> =
            vec![Value::Int32(Some(1)), Value::Varchar(Some("Alice".into()))].into();
        let row = Row::new(labels.clone(), values);
        assert_eq!(row.names(), &["id", "name"]);
        assert_eq!(row.values().len(), 2);
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn row_get_column() {
        let labels: Arc<[String]> = Arc::from(vec!["id".into(), "name".into()]);
        let values: Box<[Value]> =
            vec![Value::Int32(Some(42)), Value::Varchar(Some("Bob".into()))].into();
        let row = Row::new(labels, values);
        assert_eq!(row.get_column("id"), Some(&Value::Int32(Some(42))));
        assert_eq!(
            row.get_column("name"),
            Some(&Value::Varchar(Some("Bob".into())))
        );
        assert_eq!(row.get_column("missing"), None);
    }

    #[test]
    fn row_into_iterator() {
        let labels: Arc<[String]> = Arc::from(vec!["a".into(), "b".into()]);
        let values: Box<[Value]> = vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into();
        let row = Row::new(labels, values);
        let pairs: Vec<_> = (&row).into_iter().collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "a");
        assert_eq!(*pairs[0].1, Value::Int32(Some(1)));
    }

    #[test]
    fn row_into_iterator_mut() {
        let labels: Arc<[String]> = Arc::from(vec!["x".into()]);
        let values: Box<[Value]> = vec![Value::Int32(Some(10))].into();
        let mut row = Row::new(labels, values);
        for (_, v) in &mut row {
            *v = Value::Int32(Some(20));
        }
        assert_eq!(row.get_column("x"), Some(&Value::Int32(Some(20))));
    }

    #[test]
    fn row_values_into() {
        let labels: Arc<[String]> = Arc::from(vec!["c".into()]);
        let values: Box<[Value]> = vec![Value::Boolean(Some(true))].into();
        let row = Row::new(labels, values);
        let rv: Box<[Value]> = row.into();
        assert_eq!(rv.len(), 1);
    }

    #[test]
    fn row_to_query_result() {
        let labels: Arc<[String]> = Arc::from(vec!["id".into()]);
        let values: Box<[Value]> = vec![Value::Int32(Some(1))].into();
        let row = Row::new(labels, values);
        let result: QueryResult = row.into();
        assert!(matches!(result, QueryResult::Row(_)));
    }

    #[test]
    fn rows_affected_extend() {
        let mut a = RowsAffected {
            rows_affected: Some(5),
            last_affected_id: Some(10),
        };
        let b = RowsAffected {
            rows_affected: Some(3),
            last_affected_id: Some(20),
        };
        a.extend([b]);
        assert_eq!(a.rows_affected, Some(8));
        assert_eq!(a.last_affected_id, Some(20));
    }

    #[test]
    fn rows_affected_extend_none() {
        let mut a = RowsAffected::default();
        let b = RowsAffected::default();
        a.extend([b]);
        assert_eq!(a.rows_affected, None);
        assert_eq!(a.last_affected_id, None);
    }

    #[test]
    fn rows_affected_extend_mixed() {
        let mut a = RowsAffected {
            rows_affected: None,
            last_affected_id: None,
        };
        let b = RowsAffected {
            rows_affected: Some(7),
            last_affected_id: None,
        };
        a.extend([b]);
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
    fn dyn_query_basic_operations() {
        let mut q = DynQuery::default();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        q.push_str("SELECT ");
        assert!(!q.is_empty());
        assert_eq!(q.len(), 7);
        q.push('*');
        assert_eq!(q.len(), 8);
        assert_eq!(q.as_str().as_ref(), "SELECT *");
    }

    #[test]
    fn dyn_query_with_capacity() {
        let mut q = DynQuery::with_capacity(1024);
        assert!(q.is_empty());
        q.push_str("hello");
        assert_eq!(q.as_str().as_ref(), "hello");
    }

    #[test]
    fn dyn_query_new() {
        let q = DynQuery::new("SELECT 1".into());
        assert_eq!(q.as_str().as_ref(), "SELECT 1");
        assert_eq!(q.len(), 8);
        assert!(!q.is_empty());
    }

    #[test]
    fn dyn_query_write_trait() {
        use std::fmt::Write;
        let mut q = DynQuery::default();
        write!(q, "INSERT INTO {} VALUES ({})", "t", 42).unwrap();
        assert_eq!(q.as_str().as_ref(), "INSERT INTO t VALUES (42)");
    }

    #[test]
    fn dyn_query_into_string() {
        let q = DynQuery::new("hello world".into());
        let s: String = q.into();
        assert_eq!(s, "hello world");
    }

    #[test]
    fn dyn_query_buffer() {
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
            value_to_json(&Value::Int128(Some(999))),
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
            value_to_json(&Value::UInt128(Some(300))),
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
    fn util_value_to_json_null_variants() {
        assert_eq!(
            value_to_json(&Value::Boolean(None)),
            Some(serde_json::Value::Null)
        );
        assert_eq!(
            value_to_json(&Value::Int32(None)),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn util_value_to_json_blob() {
        let blob = Value::Blob(Some(vec![0xDE, 0xAD].into()));
        let result = value_to_json(&blob);
        assert!(result.is_some());
        let arr = result.unwrap();
        assert!(arr.is_array());
        assert_eq!(arr.as_array().unwrap().len(), 2);
    }

    #[test]
    fn util_value_to_json_uuid() {
        let uuid = uuid::Uuid::nil();
        let result = value_to_json(&Value::Uuid(Some(uuid)));
        assert_eq!(
            result,
            Some(serde_json::json!("00000000-0000-0000-0000-000000000000"))
        );
    }

    #[test]
    fn util_value_to_json_date() {
        use time::{Date, Month};
        let date = Date::from_calendar_date(2025, Month::March, 15).unwrap();
        let result = value_to_json(&Value::Date(Some(date)));
        assert!(result.is_some());
        assert!(result.unwrap().is_string());
    }

    #[test]
    fn util_value_to_json_array_and_list() {
        let arr = Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(Value::Int32(None)),
            2,
        );
        let result = value_to_json(&arr).unwrap();
        assert_eq!(result, serde_json::json!([1, 2]));

        let list = Value::List(
            Some(vec![
                Value::Varchar(Some("a".into())),
                Value::Varchar(Some("b".into())),
            ]),
            Box::new(Value::Varchar(None)),
        );
        let result = value_to_json(&list).unwrap();
        assert_eq!(result, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn util_value_to_json_map() {
        let mut map = HashMap::new();
        map.insert(Value::Int32(Some(1)), Value::Int32(Some(42)));
        let v = Value::Map(
            Some(map),
            Box::new(Value::Int32(None)),
            Box::new(Value::Int32(None)),
        );
        assert_eq!(value_to_json(&v), None);

        let empty_map: HashMap<Value, Value> = HashMap::new();
        let v2 = Value::Map(
            Some(empty_map),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        );
        let result = value_to_json(&v2).unwrap();
        assert!(result.is_object());
    }

    #[test]
    fn util_value_to_json_json() {
        let j = serde_json::json!({"nested": true});
        let result = value_to_json(&Value::Json(Some(j.clone()))).unwrap();
        assert_eq!(result, j);
    }

    #[test]
    fn util_value_to_json_struct() {
        let s = Value::Struct(Some(vec![]), vec![], TableRef::new("my_type".into()));
        let result = value_to_json(&s).unwrap();
        assert!(result.is_object());
    }

    #[test]
    fn util_value_to_json_interval_returns_none() {
        use tank::Interval;
        let v = Value::Interval(Some(Interval::from_days(1)));
        assert_eq!(value_to_json(&v), None);
    }

    #[test]
    fn util_value_to_json_nan_returns_none() {
        assert_eq!(value_to_json(&Value::Float64(Some(f64::NAN))), None);
    }

    #[test]
    fn util_value_to_json_decimal() {
        use rust_decimal::Decimal;
        let d = Decimal::new(1234, 2);
        let result = value_to_json(&Value::Decimal(Some(d), 0, 0));
        assert!(result.is_some());
    }

    #[test]
    fn util_write_escaped() {
        use tank::write_escaped;
        let mut q = DynQuery::default();
        write_escaped(&mut q, "hello", '\'', "''");
        assert_eq!(q.as_str().as_ref(), "hello");

        let mut q2 = DynQuery::default();
        write_escaped(&mut q2, "it's a test", '\'', "''");
        assert_eq!(q2.as_str().as_ref(), "it''s a test");

        let mut q3 = DynQuery::default();
        write_escaped(&mut q3, "a\"b\"c", '"', "\"\"");
        assert_eq!(q3.as_str().as_ref(), "a\"\"b\"\"c");
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
        writer.write_create_schema::<SchemaEntity>(&mut q, true);
        assert_eq!(
            q.as_str().as_ref(),
            "CREATE SCHEMA IF NOT EXISTS \"analytics\";"
        );

        let mut q2 = DynQuery::default();
        writer.write_create_schema::<SchemaEntity>(&mut q2, false);
        assert_eq!(q2.as_str().as_ref(), "CREATE SCHEMA \"analytics\";");

        let mut q3 = DynQuery::default();
        writer.write_drop_schema::<SchemaEntity>(&mut q3, true);
        assert_eq!(q3.as_str().as_ref(), "DROP SCHEMA IF EXISTS \"analytics\";");
        let mut q4 = DynQuery::default();
        writer.write_drop_schema::<SchemaEntity>(&mut q4, false);
        assert_eq!(q4.as_str().as_ref(), "DROP SCHEMA \"analytics\";");
    }

    #[test]
    fn sql_writer_select_star() {
        use tank::QueryBuilder;
        #[derive(Entity)]
        struct StarTable {
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();

        let empty_cols: &[&dyn tank::Expression] = &[];
        writer.write_select(
            &mut q,
            &QueryBuilder::new()
                .select(empty_cols)
                .from(StarTable::table()),
        );
        assert!(q.as_str().contains("SELECT *"));
    }

    #[test]
    fn sql_writer_write_map_value() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let mut map = HashMap::new();
        map.insert(Value::Varchar(Some("key1".into())), Value::Int32(Some(100)));
        let map_val = Value::Map(
            Some(map),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        );
        writer.write_value(&mut Default::default(), &mut q, &map_val);
        let s = q.as_str().to_string();
        assert!(s.contains("key1"));
        assert!(s.contains("100"));
    }

    #[test]
    fn sql_writer_write_struct_value() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let struct_val = Value::Struct(
            Some(vec![
                ("name".into(), Value::Varchar(Some("Alice".into()))),
                ("age".into(), Value::Int32(Some(30))),
            ]),
            vec![],
            TableRef::new("person".into()),
        );
        writer.write_value(&mut Default::default(), &mut q, &struct_val);
        let s = q.as_str().to_string();
        assert!(s.contains("Alice"));
        assert!(s.contains("30"));
    }

    #[test]
    fn sql_writer_write_json_value() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let json_val = Value::Json(Some(serde_json::json!({"active": true})));
        writer.write_value(&mut Default::default(), &mut q, &json_val);
        let s = q.as_str().to_string();
        assert!(s.contains("active"));
    }

    #[test]
    fn sql_writer_write_blob_value() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let blob_val = Value::Blob(Some(vec![0xCA, 0xFE].into()));
        writer.write_value(&mut Default::default(), &mut q, &blob_val);
        let s = q.as_str().to_string();
        assert!(s.contains("CA"));
        assert!(s.contains("FE"));
    }

    #[test]
    fn sql_writer_write_timestamptz() {
        use time::{Date, Month, OffsetDateTime, Time, UtcOffset};
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let dt = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(10, 30, 0).unwrap(),
            UtcOffset::from_hms(5, 30, 0).unwrap(),
        );
        let val = Value::TimestampWithTimezone(Some(dt));
        writer.write_value(&mut Default::default(), &mut q, &val);
        let s = q.as_str().to_string();
        assert!(s.contains("2025"));
        assert!(s.contains("10:30"));
    }

    #[test]
    fn sql_writer_write_infinity_float() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let val = Value::Float64(Some(f64::INFINITY));
        writer.write_value(&mut Default::default(), &mut q, &val);
        let s = q.as_str().to_string();
        assert!(s.contains("CAST"));
        assert!(s.contains("inf"));
    }

    #[test]
    fn sql_writer_write_nan_float() {
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let val = Value::Float32(Some(f32::NAN));
        writer.write_value(&mut Default::default(), &mut q, &val);
        let s = q.as_str().to_string();
        assert!(s.contains("CAST"));
        assert!(s.contains("NaN"));
    }

    #[test]
    fn sql_writer_transaction_statements() {
        let writer = GenericSqlWriter::new();
        let mut q1 = DynQuery::default();
        writer.write_transaction_begin(&mut q1);
        assert_eq!(q1.as_str().as_ref(), "BEGIN;");

        let mut q2 = DynQuery::default();
        writer.write_transaction_commit(&mut q2);
        assert_eq!(q2.as_str().as_ref(), "COMMIT;");

        let mut q3 = DynQuery::default();
        writer.write_transaction_rollback(&mut q3);
        assert_eq!(q3.as_str().as_ref(), "ROLLBACK;");
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
        writer.write_insert(&mut q, entities, false);
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
            &tank::QueryBuilder::new()
                .select(AppendTable::columns())
                .from(AppendTable::table()),
        );
        let s = q.as_str().to_string();
        assert!(s.starts_with("-- comment\nSELECT"));
    }

    #[test]
    fn sql_writer_do_nothing_on_pk_only() {
        #[derive(Entity)]
        struct PkOnly {
            #[tank(primary_key)]
            id: i32,
        }
        let writer = GenericSqlWriter::new();
        let mut q = DynQuery::default();
        let entity = PkOnly { id: 1 };
        writer.write_insert(&mut q, [&entity], true);
        let s = q.as_str().to_string();
        assert!(s.contains("DO NOTHING"));
    }

    #[test]
    fn util_either_iterator() {
        use tank::EitherIterator;
        let left: EitherIterator<std::vec::IntoIter<i32>, std::vec::IntoIter<i32>> =
            EitherIterator::Left(vec![1, 2, 3].into_iter());
        assert_eq!(left.collect::<Vec<_>>(), vec![1, 2, 3]);

        let right: EitherIterator<std::vec::IntoIter<i32>, std::vec::IntoIter<i32>> =
            EitherIterator::Right(vec![4, 5].into_iter());
        assert_eq!(right.collect::<Vec<_>>(), vec![4, 5]);
    }

    #[test]
    fn util_consume_while() {
        use tank_core::consume_while;
        let mut input = "12345abc";
        let digits = consume_while(&mut input, |c| c.is_ascii_digit());
        assert_eq!(digits, "12345");
        assert_eq!(input, "abc");

        let mut input2 = "abc";
        let empty = consume_while(&mut input2, |c| c.is_ascii_digit());
        assert_eq!(empty, "");
        assert_eq!(input2, "abc");
    }

    #[test]
    fn util_extract_number() {
        use tank_core::extract_number;

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
        use tank_core::as_c_string;
        let cs = as_c_string("hello");
        assert_eq!(cs.to_str().unwrap(), "hello");

        let cs2 = as_c_string(vec![104, 0, 105]);
        assert_eq!(cs2.to_str().unwrap(), "h?i");
    }

    #[test]
    fn util_separated_by() {
        use tank_core::separated_by;
        let mut out = DynQuery::default();
        separated_by(
            &mut out,
            [1, 2, 3],
            |o, v| {
                use std::fmt::Write;
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
                use std::fmt::Write;
                let _ = write!(o, "{v}");
            },
            ", ",
        );
        assert_eq!(out2.as_str(), "42");
    }

    #[test]
    fn util_column_def() {
        use tank_core::column_def;
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
    fn context_constructors() {
        use tank::{Context, Fragment};
        let c = Context::empty();
        assert_eq!(c.fragment, Fragment::None);
        assert!(!c.qualify_columns);
        assert!(!c.quote_identifiers);

        let c2 = Context::fragment(Fragment::SqlSelect);
        assert_eq!(c2.fragment, Fragment::SqlSelect);
        assert!(!c2.qualify_columns);
        assert!(c2.quote_identifiers);

        let c3 = Context::qualify(true);
        assert!(c3.qualify_columns);

        let c4 = Context::qualify_with("my_table".into());
        assert!(c4.qualify_columns);
    }

    #[test]
    fn context_switch_fragment() {
        use tank::{Context, Fragment};
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        ctx.counter = 5;
        {
            let mut updater = ctx.switch_fragment(Fragment::SqlSelectWhere);
            assert_eq!(updater.current.fragment, Fragment::SqlSelectWhere);
            updater.current.counter = 10;
        }
        assert_eq!(ctx.counter, 10);
        assert_eq!(ctx.fragment, Fragment::SqlSelect);
    }

    #[test]
    fn context_switch_table() {
        use tank::{Context, Fragment};
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        {
            let updater = ctx.switch_table(TableRef::new("my_tbl".into()));
            assert!(updater.current.qualify_columns);
        }
        {
            let updater = ctx.switch_table(TableRef::new("".into()));
            assert!(!updater.current.qualify_columns);
        }
    }

    #[test]
    fn context_update_from() {
        use tank::{Context, Fragment};
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        let other = Context {
            counter: 42,
            ..Context::new(Fragment::SqlSelect, false)
        };
        ctx.update_from(&other);
        assert_eq!(ctx.counter, 42);
    }

    #[test]
    fn join_qualified_columns() {
        use tank::Dataset;
        #[derive(Entity)]
        #[tank(schema = "sch")]
        struct Tbl1 {
            _x: i32,
        }
        #[derive(Entity)]
        struct Tbl2 {
            _y: i32,
        }
        let join = tank::Join::<_, _, ()> {
            join: tank::JoinType::Inner,
            lhs: Tbl1::table().clone(),
            rhs: Tbl2::table().clone(),
            on: None,
        };

        let tr = join.table_ref();
        assert!(tr.name.is_empty());
    }

    #[test]
    fn join_table_ref_same_schema() {
        use tank::Dataset;
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
        let join = tank::Join::<_, _, ()> {
            join: tank::JoinType::Left,
            lhs: Left1::table().clone(),
            rhs: Right1::table().clone(),
            on: None,
        };
        let tr = join.table_ref();

        assert_eq!(tr.schema.as_ref(), "common");
    }

    #[test]
    fn relations_fixed_decimal() {
        use rust_decimal::Decimal;
        use tank_core::FixedDecimal;
        let fd: FixedDecimal<10, 2> = Decimal::from(42).into();
        let back: Decimal = fd.into();
        assert_eq!(back, Decimal::from(42));
    }

    #[test]
    fn relations_references() {
        use tank_core::References;
        #[derive(Entity)]
        struct RefTarget {
            _id: i32,
        }
        let refs = References::<RefTarget>::new(Box::new([]));
        assert_eq!(refs.table_ref().name.as_ref(), "ref_target");
        assert!(refs.columns().is_empty());
    }
}
