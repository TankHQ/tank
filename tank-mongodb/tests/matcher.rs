#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};
    use tank::{ColumnRef, Entity, Expression, expr};
    use tank_mongodb::{IsColumn, IsFieldCondition, MongoDBSqlWriter};
    use tank_tests::init_logs;

    #[derive(Entity)]
    struct Table {
        pub col_a: i64,
        #[tank(name = "second_column")]
        pub col_b: i128,
        pub str_column: String,
    }
    const WRITER: MongoDBSqlWriter = MongoDBSqlWriter {};

    #[test]
    fn is_column() {
        init_logs();
        {
            let mut matcher = IsColumn::default();
            assert!(matcher.column.is_none());
            assert!(Table::col_a.matches(&mut matcher, &WRITER));
            assert_eq!(matcher.column, Some(Table::col_a));
            assert_ne!(matcher.column, Some(Table::col_b));
        }
        {
            let mut matcher = IsColumn::default();
            assert!(expr!(table.col_a).matches(&mut matcher, &WRITER));
            assert_eq!(
                matcher.column,
                Some(ColumnRef {
                    name: "col_a".into(),
                    table: "table".into(),
                    schema: "".into()
                })
            );
        }
        assert!(!true.matches(&mut IsColumn::default(), &WRITER));
    }

    #[test]
    fn is_field_condition() {
        init_logs();
        {
            let mut matcher = IsFieldCondition::default();
            assert!(matcher.condition.is_empty());
            assert!(expr!(Table::col_b == 41).matches(&mut matcher, &WRITER));
            assert_eq!(matcher.condition, doc! { "second_column": Bson::Int64(41) });
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(10 < Table::col_a).matches(&mut matcher, &WRITER));
            assert_eq!(
                matcher.condition,
                doc! { "col_a": { "$gt": Bson::Int64(10) } }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(Table::str_column == "hello world").matches(&mut matcher, &WRITER));
            assert_eq!(
                matcher.condition,
                doc! { "str_column": Bson::String("hello world".to_string()) }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(Table::col_a != 100).matches(&mut matcher, &WRITER));
            assert_eq!(
                matcher.condition,
                doc! { "col_a": { "$ne": Bson::Int64(100) } }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(
                expr!(Table::col_a <= 5 && Table::str_column == "hello" && Table::col_b == 42)
                    .matches(&mut matcher, &WRITER)
            );
            assert_eq!(
                matcher.condition,
                doc! {
                    "$and": [
                        { "col_a": { "$le": Bson::Int64(5) } },
                        { "str_column": "hello" },
                        { "second_column": Bson::Int64(42) },
                    ]
                }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(
                expr!(0 > Table::col_a || Table::str_column == "world")
                    .matches(&mut matcher, &WRITER)
            );
            assert_eq!(
                matcher.condition,
                doc! {
                    "$or": [
                        { "col_a": { "$lt": Bson::Int64(0) } },
                        { "str_column": "world" },
                    ]
                }
            );
        }
    }
}
