#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};
    use std::borrow::Cow;
    use tank::{Entity, QueryBuilder, cols, expr};
    use tank_mongodb::{AggregatePayload, MongoDBDriver, Payload};
    use tank_tests::init_logs;

    const DRIVER: MongoDBDriver = MongoDBDriver {};

    #[test]
    fn pipeline_1() {
        #[derive(Entity)]
        #[tank(name = "the table")]
        struct MyType {
            #[tank(name = "first col")]
            pub first_column: Cow<'static, str>,
            pub second_column: Option<f64>,
            pub third_column: String,
        }
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
                },
                doc! {
                    "$group": {
                        "_id": {
                            "first col": "$first col",
                            "third_column": "$third_column",
                        },
                        "MAX(second_column)": { "$max": "$second_column" },
                        "AVG(second_column)": { "$avg": "$second_column" },
                    }
                },
                doc! {
                    "$match": {
                        "_id.first col": { "$gte": "a" },
                    }
                },
                doc! {
                    "$project": {
                        "first col": "$_id.first col",
                        "MAX(second_column)": 1,
                        "AVG(second_column)": 1,
                    }
                },
            ]
        );
    }

    #[test]
    fn pipeline_2() {
        #[derive(Entity)]
        #[tank(name = "shopping carts")]
        struct Cart {
            #[tank(name = "_id")]
            pub id: i64,
            #[tank(name = "user id")]
            pub user_id: i64,
            #[tank(name = "is active")]
            pub is_active: bool,
            #[tank(name = "total price")]
            pub total_price: f64,
            pub discounts: Vec<f64>,
            pub country: Cow<'static, str>,
        }
        let mut query = QueryBuilder::new()
            .select(cols!(
                Cart::user_id,
                COUNT(Cart::id),
                SUM(Cart::total_price),
                AVG(Cart::total_price),
                MAX(expr!(ABS(Cart::total_price - 100.0))),
            ))
            .from(Cart::table())
            .where_expr(expr!(
                Cart::is_active == true
                    && Cart::total_price > 0
                    && Cart::total_price < 10_000
                    && (Cart::country == "US" || Cart::country == "FR" || Cart::country == "DE")
                    && expr!([10, 20, 30, 40][2]) == 30
                    && (90.5 - -0.54 * 2 < 7 / 2)
            ))
            .group_by([Cart::user_id, Cart::country])
            .having(expr!(
                COUNT(Cart::id) > 2
                    && AVG(Cart::total_price) >= 50
                    && SUM(Cart::total_price) < 50_000
                    && MAX(expr!(ABS(Cart::total_price - 100.0))) > 10
            ))
            .limit(Some(1000))
            .build(&DRIVER);

        let Some(Payload::Aggregate(AggregatePayload { pipeline, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected aggregate pipeline");
        };

        assert_eq!(
            *pipeline,
            [
                doc! {
                    "$match": {
                        "$and": [
                            { "is active": true },
                            { "total price": { "$gt": Bson::Int64(0) } },
                            { "total price": { "$lt": Bson::Int64(10_000) } },
                            {
                                "$or": [
                                    { "country": "US" },
                                    { "country": "FR" },
                                    { "country": "DE" },
                                ]
                            },
                            {
                                "$expr": {
                                    "$eq": [
                                        {
                                            "$arrayElemAt": [
                                                [
                                                    Bson::Int64(10),
                                                    Bson::Int64(20),
                                                    Bson::Int64(30),
                                                    Bson::Int64(40),
                                                ],
                                                Bson::Int64(2)
                                            ]
                                        },
                                        Bson::Int64(30)
                                    ]
                                }
                            },
                            {
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
                        ]
                    }
                },
                doc! {
                    "$group": {
                        "_id": {
                            "user id": "$user id",
                            "country": "$country",
                        },
                        "COUNT(_id)": { "$sum": Bson::Int64(1) },
                        "SUM(total price)": { "$sum": "$total price" },
                        "AVG(total price)": { "$avg": "$total price" },
                        "MAX(ABS(total price - 100.0))": {
                            "$max": {
                                "$abs": {
                                    "$subtract": ["$total price", 100.0]
                                }
                            }
                        }
                    }
                },
                doc! {
                    "$match": {
                        "$and": [
                            { "COUNT(_id)": { "$gt": 2 } },
                            { "AVG(total price)": { "$gte": Bson::Int64(50) } },
                            { "SUM(total price)": { "$lt": Bson::Int64(50000) } },
                            { "MAX(ABS(total price - 100.0))": { "$gt": Bson::Int64(10) } },
                        ]
                    }
                },
                doc! {"$limit": 1000},
                doc! {
                    "$project": {
                        "user id": "$_id.user id",
                        "country": "$_id.country",
                        "COUNT(_id)": 1,
                        "SUM(total price)": 1,
                        "AVG(total price)": 1,
                        "MAX(ABS(total price - 100.0))": 1,
                    }
                },
            ]
        );
    }
}
