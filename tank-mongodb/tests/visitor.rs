#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};
    use tank::{Context, Entity, Expression, TableRef, expr};
    use tank_mongodb::{
        IsCount, MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter, WriteMatchExpression,
    };
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
    fn write_match_expression() {
        init_logs();
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(Table::col_b == 41).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! { "second_column": Bson::Int64(41) }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(10 < Table::col_a).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context {
                    table_ref: TableRef::new("_id".into()),
                    qualify_columns: true,
                    quote_identifiers: false,
                    ..Default::default()
                },
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! { "_id.col_a": { "$gt": Bson::Int64(10) } }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(Table::str_column == "hello world").accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context {
                    quote_identifiers: false,
                    ..Default::default()
                },
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! { "table.str_column": Bson::String("hello world".to_string()) }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(Table::col_a != 100).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! { "col_a": { "$ne": Bson::Int64(100) } }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(Table::col_a <= 5 && Table::str_column == "hello" && Table::col_b == 42)
                .accept_visitor(
                    &mut WriteMatchExpression::new(),
                    &WRITER,
                    &mut Context {
                        table_ref: TableRef::new("prefix".into()),
                        quote_identifiers: false,
                        ..Default::default()
                    },
                    &mut out,
                );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$and": [
                        { "prefix.col_a": { "$lte": Bson::Int64(5) } },
                        { "prefix.str_column": "hello" },
                        { "prefix.second_column": Bson::Int64(42) },
                    ]
                }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(0 > Table::col_a || Table::str_column == "world").accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$or": [
                        { "col_a": { "$lt": Bson::Int64(0) } },
                        { "str_column": "world" },
                    ]
                }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(
                0 <= Table::col_a
                    && Table::col_a <= 999
                    && Table::str_column == "hello"
                    && Table::col_a != 777
            )
            .accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context {
                    table_ref: TableRef::new("hello world".into()),
                    quote_identifiers: false,
                    ..Default::default()
                },
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$and": [
                        { "hello world.col_a": { "$gte": Bson::Int64(0) } },
                        { "hello world.col_a": { "$lte": Bson::Int64(999) } },
                        { "hello world.str_column": "hello" },
                        { "hello world.col_a": { "$ne": Bson::Int64(777) } },
                    ]
                }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(alpha > Table::col_b).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$expr": {
                        "$gt": ["$alpha", "$second_column"]
                    }
                }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(Table::str_column == (?, ?) as IN && Table::col_a > ?).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$expr": {
                        "$and": [
                            {
                                "$in": [
                                    "$str_column",
                                    ["$$param_0", "$$param_1"]
                                ]
                            },
                            { "$gt": ["$col_a", "$$param_2"] },
                        ]
                    }
                }
            );
        }
        {
            let mut out = MongoDBSqlWriter::make_prepared();
            expr!(90.5 - -0.54 * 2 < 7 / 2).accept_visitor(
                &mut WriteMatchExpression::new(),
                &WRITER,
                &mut Context::empty(),
                &mut out,
            );
            assert_eq!(
                *out.as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                    .and_then(Bson::as_document_mut)
                    .expect("Wrong result type"),
                doc! {
                    "$expr": {
                        "$lt": [
                            {
                                "$subtract": [
                                    90.5,
                                    { "$multiply": [-0.54, Bson::Int64(2)] }
                                ]
                            },
                            { "$divide": [Bson::Int64(7), Bson::Int64(2)] }
                        ]
                    }
                }
            );
        }
    }

    #[test]
    fn is_count() {
        init_logs();
        let mut matcher = IsCount::default();
        assert!(expr!(COUNT(*)).accept_visitor(
            &mut matcher,
            &WRITER,
            &mut Default::default(),
            &mut Default::default(),
        ));
        assert!(!expr!(COUNT(hello)).accept_visitor(
            &mut matcher,
            &WRITER,
            &mut Default::default(),
            &mut Default::default(),
        ));
        assert!(!expr!(25 == 1).accept_visitor(
            &mut matcher,
            &WRITER,
            &mut Default::default(),
            &mut Default::default(),
        ));
    }
}
