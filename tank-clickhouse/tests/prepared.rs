#[cfg(test)]
mod tests {
    use tank_clickhouse::{ClickHousePrepared, ClickHouseSqlWriter};
    use tank_core::{
        Context, DynQuery, Expression, Fragment, Prepared, QueryParam, RawQuery, Value,
    };

    fn render(expression: &dyn Expression) -> RawQuery {
        let writer = ClickHouseSqlWriter::new();
        let mut out = DynQuery::default();
        let mut context = Context::fragment(Fragment::SqlSelect);
        expression.write_query(&writer, &mut context, &mut out);
        let DynQuery::Raw(raw) = out else {
            panic!("the writer only produces raw queries");
        };
        raw
    }

    #[test]
    fn build_sql_replaces_markers_and_types() {
        let writer = ClickHouseSqlWriter::new();
        let RawQuery { sql, params } = render(&tank::expr!(a == ? && b == ?));
        let mut prepared = ClickHousePrepared::new(sql, params);
        prepared.bind(1_i64).unwrap();
        prepared.bind("x").unwrap();
        let (directive, built) = prepared.build_sql(&writer).unwrap();
        let directive = directive.expect("bound values must produce a SET directive");
        assert!(directive.starts_with("SET param_p0 = "), "{directive}");
        assert!(directive.contains("param_p1 = "), "{directive}");
        assert!(built.contains("{p0:Nullable(Int64)}"), "{built}");
        assert!(built.contains("{p1:Nullable(String)}"), "{built}");
    }

    #[test]
    fn build_sql_without_markers_is_unchanged() {
        let writer = ClickHouseSqlWriter::new();
        let prepared = ClickHousePrepared::new("SELECT 1".to_string(), vec![]);
        let (directive, built) = prepared.build_sql(&writer).unwrap();
        assert!(directive.is_none());
        assert_eq!(built, "SELECT 1");
    }

    #[test]
    fn binding_beyond_marker_count_errors() {
        let mut prepared = ClickHousePrepared::new("SELECT 1".to_string(), vec![]);
        assert!(prepared.bind(1_i64).is_err());
    }

    #[test]
    fn null_value_renders_nullable_nothing() {
        let writer = ClickHouseSqlWriter::new();
        let RawQuery { sql, params } = render(&tank::expr!(a == ?));
        let mut prepared = ClickHousePrepared::new(sql, params);
        prepared.bind(Option::<i64>::None).unwrap();
        let (directive, built) = prepared.build_sql(&writer).unwrap();
        let directive = directive.expect("a NULL binding still produces a SET directive");
        assert!(directive.contains("'\\\\N'"), "{directive}");
        assert!(built.contains("Nullable(Nothing)"), "{built}");
    }

    #[test]
    fn bind_index_and_reuse() {
        let writer = ClickHouseSqlWriter::new();
        let RawQuery { sql, params } = render(&tank::expr!(a == ? && b == ?));
        let mut prepared = ClickHousePrepared::new(sql, params);
        prepared.bind_index(10_i32, 1).unwrap();
        prepared.bind_index(20_i32, 0).unwrap();
        let (_, built) = prepared.build_sql(&writer).unwrap();
        assert!(built.contains("Nullable(Int32)}"), "{built}");

        let taken = prepared.take_params();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0], Value::Int32(Some(20)));
        let (directive, built) = prepared.build_sql(&writer).unwrap();
        assert!(directive.is_none());
        assert!(built.contains("Nullable(Nothing)"), "{built}");

        prepared.bind(1_i64).unwrap();
        prepared.clear_bindings().unwrap();
        let (directive, _) = prepared.build_sql(&writer).unwrap();
        assert!(directive.is_none());
    }

    #[test]
    fn malformed_markers_do_not_panic() {
        let writer = ClickHouseSqlWriter::new();
        let RawQuery { sql, params } = render(&tank::expr!(a == ? && b == ?));
        let reversed: Vec<QueryParam> = params.into_iter().rev().collect();
        let mut prepared = ClickHousePrepared::new(sql, reversed);
        prepared.bind(1_i64).unwrap();
        let (_, built) = prepared.build_sql(&writer).unwrap();
        assert!(built.starts_with("a = "), "{built}");
    }
}
