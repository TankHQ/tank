#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};
    use tank::{Entity, Expression, expr};
    use tank_mongodb::{IsCount, IsFieldCondition, MongoDBSqlWriter};
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
    fn is_field_condition() {
        init_logs();
        {
            let mut matcher = IsFieldCondition::default();
            assert!(matcher.condition.is_empty());
            assert!(expr!(Table::col_b == 41).matches(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
            ));
            assert_eq!(matcher.condition, doc! { "second_column": Bson::Int64(41) });
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(10 < Table::col_a).matches(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
            ));
            assert_eq!(
                matcher.condition,
                doc! { "col_a": { "$gt": Bson::Int64(10) } }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(Table::str_column == "hello world").matches(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
            ));
            assert_eq!(
                matcher.condition,
                doc! { "str_column": Bson::String("hello world".to_string()) }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(expr!(Table::col_a != 100).matches(
                &mut matcher,
                &WRITER,
                &mut Default::default(),
            ));
            assert_eq!(
                matcher.condition,
                doc! { "col_a": { "$ne": Bson::Int64(100) } }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(
                expr!(Table::col_a <= 5 && Table::str_column == "hello" && Table::col_b == 42)
                    .matches(&mut matcher, &WRITER, &mut Default::default())
            );
            assert_eq!(
                matcher.condition,
                doc! {
                    "$and": [
                        { "col_a": { "$lte": Bson::Int64(5) } },
                        { "str_column": "hello" },
                        { "second_column": Bson::Int64(42) },
                    ]
                }
            );
        }
        {
            let mut matcher = IsFieldCondition::default();
            assert!(
                expr!(0 > Table::col_a || Table::str_column == "world").matches(
                    &mut matcher,
                    &WRITER,
                    &mut Default::default(),
                )
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
        {
            let mut matcher = IsFieldCondition::default();
            assert!(
                expr!(
                    0 <= Table::col_a
                        && Table::col_a <= 999
                        && Table::str_column == "hello"
                        && Table::col_a != 777
                )
                .matches(&mut matcher, &WRITER, &mut Default::default())
            );
            assert_eq!(
                matcher.condition,
                doc! {
                    "$and": [
                        { "col_a": { "$gte": Bson::Int64(0) } },
                        { "col_a": { "$lte": Bson::Int64(999) } },
                        { "str_column": "hello" },
                        { "col_a": { "$ne": Bson::Int64(777) } },
                    ]
                }
            );
        }
    }

    #[test]
    fn is_count() {
        init_logs();
        let mut matcher = IsCount::default();
        assert!(expr!(COUNT(*)).matches(&mut matcher, &WRITER, &mut Default::default()));
        assert!(!expr!(COUNT(hello)).matches(&mut matcher, &WRITER, &mut Default::default()));
        assert!(!expr!(25 == 1).matches(&mut matcher, &WRITER, &mut Default::default()));
    }
}
