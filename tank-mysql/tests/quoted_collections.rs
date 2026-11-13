#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use tank::{
        CollectionType, Context, Driver, DynQuery, Fragment, MapType, SqlValueWriter, Value,
    };
    use tank_mysql::MySQLDriver;
    use tank_tests::init_logs;

    fn render(v: &Value) -> String {
        let writer = MySQLDriver::default().sql_writer();
        let mut out = DynQuery::default();
        writer.write_value(
            &mut Context::new(Fragment::SqlInsertIntoValues, false),
            &mut out,
            v,
        );
        out.as_str().into_owned()
    }

    #[test]
    fn list_with_quote_is_escaped() {
        init_logs();
        let v = Value::List(
            Some(
                vec![
                    Value::Varchar(Some("O'Brien".into())),
                    Value::Varchar(Some("x".into())),
                ]
                .into(),
            ),
            Box::new(CollectionType {
                element: Value::Varchar(None),
                len: 0,
            }),
        );
        assert_eq!(render(&v), "'[\"O''Brien\",\"x\"]'");
    }

    #[test]
    fn map_with_quote_is_escaped() {
        init_logs();
        let mut m = HashMap::new();
        m.insert(
            Value::Varchar(Some("a'b".into())),
            Value::Varchar(Some("x".into())),
        );
        let v = Value::Map(
            Some(Box::new(m)),
            Box::new(MapType {
                key: Value::Varchar(None),
                value: Value::Varchar(None),
            }),
        );
        assert_eq!(render(&v), "'{\"a''b\":\"x\"}'");
    }
}
