mod comments;
mod structure;

#[cfg(test)]
mod tests {
    use crate::{comments::comments, structure::structure};
    use rust_decimal::Decimal;
    use std::{
        collections::{BTreeMap, HashMap},
        path::Path,
        sync::Mutex,
    };
    use tank::{
        BinaryOp, BinaryOpType, ColumnDef, Connection, ConnectionPool, Context, Driver, DynQuery,
        Executor, Expression, Fragment, GenericSqlWriter, Interval, MapType, Operand, PoolConfig,
        Prepared, Query, Row, SqlWriter, Value, stream::TryStreamExt,
    };
    use tank_duckdb::{DuckDBConnection, DuckDBDriver};
    use tank_tests::{execute_tests, init_logs};
    use tokio::fs;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[tokio::test]
    pub async fn duckdb() {
        init_logs();
        const DB_PATH: &'static str = "../target/debug/tests.duckdb";
        let _guard = MUTEX.lock().unwrap();
        if Path::new(DB_PATH).exists() {
            fs::remove_file(DB_PATH).await.expect(
                format!("Failed to remove existing test database file {}", DB_PATH).as_str(),
            );
        }
        assert!(
            !Path::new(DB_PATH).exists(),
            "Database file should not exist before test"
        );
        let url = format!("duckdb://{}?mode=rw", DB_PATH);
        let driver = DuckDBDriver::new();
        let mut pool = driver
            .connect_pool(url, PoolConfig::new())
            .await
            .expect("Could not open the database");
        let _ = pool.get().await.expect("Could not get a DuckDB connection");
        assert!(
            Path::new(DB_PATH).exists(),
            "Database file should be created after connection"
        );
        execute_tests(&mut pool).await;
        structure(&mut pool).await;
        comments(&mut pool).await;
    }

    fn duckdb_writer() -> impl SqlWriter {
        DuckDBDriver::new().sql_writer()
    }

    fn duckdb_render(value: Value<'_>) -> String {
        let mut out = DynQuery::default();
        duckdb_writer().write_value(
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
            &value,
        );
        out.into()
    }

    #[test]
    fn duckdb_blob_literals() {
        assert_eq!(
            duckdb_render(Value::Blob(Some(vec![0xDE, 0xAD].into()))),
            "'\\xDE\\xAD'"
        );
        assert_eq!(duckdb_render(Value::Blob(Some(vec![].into()))), "''");
    }

    #[test]
    fn duckdb_map_literal() {
        let writer = duckdb_writer();
        let mut map = HashMap::new();
        map.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        let mut out = DynQuery::default();
        writer.write_value(
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
            &Value::Map(
                Some(Box::new(map)),
                Box::new(MapType {
                    key: Value::Varchar(None),
                    value: Value::Int32(None),
                }),
            ),
        );
        assert_eq!(out.as_str(), "MAP{'k':1}");
    }

    #[test]
    fn duckdb_not_glob_wrapping() {
        let writer = duckdb_writer();
        let mut out = DynQuery::default();
        BinaryOp {
            op: BinaryOpType::NotGlob,
            lhs: Operand::LitIdent("a"),
            rhs: Operand::LitStr("x"),
        }
        .write_query(
            &writer,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
        );
        assert_eq!(out.as_str(), "NOT (a GLOB 'x')");

        let mut out = DynQuery::default();
        BinaryOp {
            op: BinaryOpType::Glob,
            lhs: Operand::LitIdent("a"),
            rhs: Operand::LitStr("x"),
        }
        .write_query(
            &writer,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
        );
        assert_eq!(out.as_str(), "a GLOB 'x'");
    }

    #[test]
    fn duckdb_current_timestamp() {
        let writer = duckdb_writer();
        let mut out = DynQuery::default();
        writer.write_current_timestamp_ms(&mut Context::empty(), &mut out);
        assert_eq!(out.as_str(), "epoch_ms(current_timestamp)");
    }

    #[test]
    fn duckdb_interval_units() {
        assert_eq!(
            duckdb_render(Value::Interval(Some(Interval::from_days(2)))),
            "INTERVAL '2 DAYS'"
        );
        assert_eq!(
            duckdb_render(Value::Interval(Some(Interval::from_hours(3)))),
            "INTERVAL '3 HOURS'"
        );
        assert_eq!(
            duckdb_render(Value::Interval(Some(Interval::from_micros(5)))),
            "INTERVAL '5 MICROSECONDS'"
        );
    }

    #[test]
    fn duckdb_column_type_override() {
        let writer = duckdb_writer();
        let column = ColumnDef::default();
        let mut types = BTreeMap::new();
        types.insert("sqlite", "IGNORED");
        let mut out = DynQuery::default();
        writer.write_column_overridden_type(
            &mut Context::new(Fragment::SqlCreateTable, false),
            &mut out,
            &column,
            &types,
        );
        assert!(out.is_empty());

        let mut types = BTreeMap::new();
        types.insert("duckdb", "CUSTOM");
        let mut out = DynQuery::default();
        writer.write_column_overridden_type(
            &mut Context::new(Fragment::SqlCreateTable, false),
            &mut out,
            &column,
            &types,
        );
        assert_eq!(out.as_str(), "CUSTOM");
    }

    #[test]
    fn duckdb_generic_values() {
        let writer = duckdb_writer();
        let generic = GenericSqlWriter::new();
        for value in [
            Value::Int32(Some(7)),
            Value::Varchar(Some("a".into())),
            Value::Boolean(Some(true)),
        ] {
            let mut out = DynQuery::default();
            writer.write_value(
                &mut Context::new(Fragment::SqlSelect, false),
                &mut out,
                &value,
            );
            let mut expected = DynQuery::default();
            generic.write_value(
                &mut Context::new(Fragment::SqlSelect, false),
                &mut expected,
                &value,
            );
            assert_eq!(out.as_str(), expected.as_str());
        }
    }

    #[tokio::test]
    async fn bare_decimal_binding() {
        // A bare `Decimal` decodes to `Value::Decimal(.., 0, scale)`. Binding it
        // used to hand DuckDB a `DECIMAL(width=0, scale=n)` type, tripping the
        // `width >= scale` assertion and aborting the whole process.
        let mut connection = DuckDBConnection::connect(
            &DuckDBDriver::new(),
            "duckdb://:memory:?threads=1".into(),
        )
        .await
        .unwrap();
        for decimal in [
            Decimal::new(125, 2),
            Decimal::new(-1, 0),
            Decimal::new(123_456_789, 4),
        ] {
            let query = connection.prepare("SELECT ?").await.unwrap();
            let Query::Prepared(mut prepared) = query else {
                panic!("expected a prepared statement")
            };
            prepared.bind(decimal).unwrap();
            let row = {
                let rows: Vec<_> = connection
                    .fetch(Query::<DuckDBDriver>::Prepared(prepared))
                    .try_collect()
                    .await
                    .unwrap();
                rows.into_iter().next().unwrap()
            };
            let decoded = match row.value(0).map(|v| v.into_owned()) {
                Some(Value::Decimal(Some(v), ..)) => v,
                other => panic!("expected a decimal, got {other:?}"),
            };
            assert_eq!(decoded.normalize(), decimal.normalize());
        }
    }

    #[tokio::test]
    async fn bind_index_zero_based() {
        let mut connection = DuckDBConnection::connect(
            &DuckDBDriver::new(),
            "duckdb://:memory:?threads=1".into(),
        )
        .await
        .unwrap();
        // `bind_index` is 0-based; index 0 must bind the first parameter.
        let query = connection.prepare("SELECT ?, ?").await.unwrap();
        let Query::Prepared(mut prepared) = query else {
            panic!("expected a prepared statement")
        };
        prepared.bind_index(10_i32, 0).unwrap();
        prepared.bind_index(20_i32, 1).unwrap();
        let rows: Vec<_> = connection
            .fetch(Query::<DuckDBDriver>::Prepared(prepared))
            .try_collect()
            .await
            .unwrap();
        let row = &rows[0];
        assert_eq!(
            row.value(0).map(|v| v.into_owned()),
            Some(Value::Int32(Some(10)))
        );
        assert_eq!(
            row.value(1).map(|v| v.into_owned()),
            Some(Value::Int32(Some(20)))
        );
    }
}
