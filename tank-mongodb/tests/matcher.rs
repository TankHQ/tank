#[cfg(test)]
mod tests {
    use tank::{ColumnRef, Entity, Expression, expr};
    use tank_mongodb::{IsColumn, IsFieldCondition};
    use tank_tests::init_logs;

    #[derive(Entity)]
    struct Table {
        pub col_a: u32,
        pub col_b: i128,
    }

    #[test]
    fn is_column() {
        init_logs();
        {
            let mut matcher = IsColumn::default();
            assert!(matcher.column.is_none());
            assert!(Table::col_a.matches(&mut matcher));
            assert_eq!(matcher.column, Some(Table::col_a));
            assert_ne!(matcher.column, Some(Table::col_b));
        }
        {
            let mut matcher = IsColumn::default();
            assert!(expr!(table.col_a).matches(&mut matcher));
            assert_eq!(
                matcher.column,
                Some(ColumnRef {
                    name: "col_a".into(),
                    table: "table".into(),
                    schema: "".into()
                })
            );
        }
        assert!(!true.matches(&mut IsColumn::default()));
    }

    #[test]
    fn is_field_condition() {
        init_logs();
        {
            let mut matcher = IsFieldCondition::default();
            assert!(matcher.condition.is_empty());
            assert!(expr!(Table::col_b == 41).matches(&mut matcher));
            assert!(!matcher.condition.is_empty());
            let mut keys = matcher.condition.keys();
            assert_eq!(keys.next(), Some(&"col_b".to_string()));
        }
    }
}
