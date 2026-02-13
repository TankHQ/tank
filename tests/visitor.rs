#[cfg(test)]
mod tests {
    use tank::{ColumnRef, Entity, Expression, GenericSqlWriter, IsColumn, expr};

    #[derive(Entity)]
    struct Table {
        pub col_a: i64,
        #[tank(name = "second_column")]
        pub col_b: i128,
        pub str_column: String,
    }

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn is_column() {
        {
            let mut matcher = IsColumn::default();
            assert!(matcher.column.is_none());
            assert!(Table::col_a.accept_visitor(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
                &mut Default::default()
            ));
            assert_eq!(matcher.column, Some(Table::col_a));
            assert_ne!(matcher.column, Some(Table::col_b));
        }
        {
            let mut matcher = IsColumn::default();
            assert!(expr!(table.col_a).accept_visitor(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
                &mut Default::default()
            ));
            assert_eq!(
                matcher.column,
                Some(ColumnRef {
                    name: "col_a".into(),
                    table: "table".into(),
                    schema: "".into()
                })
            );
        }
        assert!(!true.accept_visitor(
            &mut IsColumn::default(),
            &WRITER,
            &mut Default::default(),
            &mut Default::default()
        ));
    }
}
