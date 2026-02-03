#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use mongodb::bson::{Bson, doc};
    use tank::{Entity, QueryBuilder, cols, expr};
    use tank_mongodb::{AggregatePayload, MongoDBDriver, Payload};
    use tank_tests::init_logs;

    const DRIVER: MongoDBDriver = MongoDBDriver {};

    #[derive(Entity)]
    #[tank(name = "the table")]
    struct MyType {
        #[tank(name = "first col")]
        pub first_column: Cow<'static, str>,
        pub second_column: Option<f64>,
        pub third_column: String,
    }

    #[test]
    fn pipeline_1() {
        init_logs();
        let mut query = QueryBuilder::new()
            .select(cols!(
                MyType::first_column,
                MAX(MyType::second_column),
                AVG(MyType::second_column)
            ))
            .from(MyType::table())
            .where_expr(expr!(
                MyType::third_column != "empty"
                    && MyType::second_column > 0
                    && MyType::second_column < 100
            ))
            .group_by([MyType::first_column, MyType::third_column])
            .having(expr!(MyType::first_column >= "a"))
            .build(&DRIVER);
        let Some(Payload::Aggregate(AggregatePayload { pipeline, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .and_then(|v| Some(v.get_payload()))
        else {
            panic!("The query did not produce a array as expected");
        };
        assert_eq!(
            *pipeline,
            [
                doc! {
                    "$match": {
                        "$and": [
                            { "third_column": { "$ne": "empty" } },
                            { "second_column": { "$gt": Bson::Int64(0) } },
                            { "second_column": { "$lt": Bson::Int64(100) } },
                        ]
                    }
                }
                .into(),
                doc! {
                    "$group": {
                        "_id": {
                            "first column": "$first_column",
                            "third_column": "$third_column",
                        },
                        "MAX(second_column)": { "$max": "$second_column" },
                        "AVG(second_column)": { "$avg": "$second_column" },
                    }
                }
                .into(),
                doc! {
                    "$match": {
                        "_id.first column": { "$gte": "a" },
                    }
                }
                .into(),
                doc! {
                    "$project": {
                        "_id": 0,
                        "first column": "$_id.first column",
                        "MAX(second_column)": 1,
                        "AVG(second_column)": 1,
                    }
                }
                .into()
            ]
        );
    }
}
