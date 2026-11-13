#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, Regex, doc};
    use std::borrow::Cow;
    use tank::{Entity, Sql, cols, expr};
    use tank_mongodb::{AggregatePayload, FindManyPayload, MongoDBDriver, Payload};
    use tank_tests::init_logs;

    const DRIVER: MongoDBDriver = MongoDBDriver {};

    #[test]
    fn pipeline_1() {
        init_logs();
        #[derive(Entity)]
        #[tank(name = "the table")]
        struct MyType {
            #[tank(name = "first col")]
            pub first_column: Cow<'static, str>,
            pub second_column: Option<f64>,
            pub third_column: String,
        }
        let mut query = Sql()
            .select(cols!(
                MyType::first_column,
                MAX(MyType::second_column),
                AVG(MyType::second_column)
            ))
            .from(MyType::table())
            .where_expr(expr!(
                (MyType::first_column == "A%" as LIKE || MyType::third_column != "empty")
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
                            {
                                "$or": [
                                    {
                                        "first col": {
                                            "$regex": Bson::RegularExpression(Regex {
                                                pattern: "^A.*$".into(),
                                                options: Default::default(),
                                            })
                                        }
                                    },
                                    { "third_column": { "$ne": "empty" } }
                                ]
                            },
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
                        "_id.first col": { "$gte": "a" }
                    }
                },
                doc! {
                    "$project": {
                        "first col": "$_id.first col",
                        "MAX(second_column)": "$MAX(second_column)",
                        "AVG(second_column)": "$AVG(second_column)",
                    }
                },
            ]
        );
    }

    #[test]
    fn pipeline_2() {
        init_logs();
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
        let mut query = Sql()
            .select(cols!(
                Cart::user_id,
                COUNT(Cart::id),
                SUM(Cart::total_price),
                AVG(Cart::total_price),
                MAX(ABS(Cart::total_price - 100.0)),
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
                    && MAX(ABS(Cart::total_price - 100.0)) > 10
                    && country != "FR%" as LIKE
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
                                                Bson::Int64(2),
                                            ]
                                        },
                                        Bson::Int64(30),
                                    ]
                                }
                            },
                            {
                                "$expr": {
                                    "$lt": [
                                        {
                                            "$subtract": [
                                                90.5,
                                                { "$multiply": [-0.54, Bson::Int64(2)] },
                                            ]
                                        },
                                        { "$divide": [Bson::Int64(7), Bson::Int64(2)] },
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
                                    "$subtract": ["$total price", 100.0],
                                }
                            }
                        },
                    }
                },
                doc! {
                    "$match": {
                        "$and": [
                            { "COUNT(_id)": { "$gt": Bson::Int64(2) } },
                            { "AVG(total price)": { "$gte": Bson::Int64(50) } },
                            { "SUM(total price)": { "$lt": Bson::Int64(50000) } },
                            { "MAX(ABS(total price - 100.0))": { "$gt": Bson::Int64(10) } },
                            {
                                "_id.country": {
                                    "$not": {
                                        "$regex": Bson::RegularExpression(Regex {
                                            pattern: "^FR.*$".into(),
                                            options: Default::default(),
                                        })
                                    },
                                    "$ne": Bson::Null
                                }
                            },
                        ]
                    }
                },
                doc! { "$limit": 1000 },
                doc! {
                    "$project": {
                        "user id": "$_id.user id",
                        "COUNT(_id)": "$COUNT(_id)",
                        "SUM(total price)": "$SUM(total price)",
                        "AVG(total price)": "$AVG(total price)",
                        "MAX(ABS(total price - 100.0))": "$MAX(ABS(total price - 100.0))",
                    }
                },
            ]
        );
    }

    #[test]
    fn pipeline_3() {
        init_logs();
        #[derive(Entity)]
        #[tank(name = "transactions")]
        struct Transaction {
            pub id: i64,
            pub amount: f64,
        }
        let mut query = Sql()
            .select(cols!(Transaction::id, ABS(Transaction::amount),))
            .from(Transaction::table())
            .build(&DRIVER);

        let Some(Payload::FindMany(FindManyPayload { options, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected FindMany payload (not aggregate) since ABS is scalar");
        };

        assert_eq!(
            options.projection,
            Some(doc! {
                "id": Bson::Int32(1),
                "ABS(amount)": { "$abs": "$amount" },
            })
        );
    }

    #[test]
    fn pipeline_4() {
        init_logs();
        #[derive(Entity)]
        #[tank(schema = "analytics", name = "raw telemetry")]
        struct Telemetry {
            #[tank(name = "_id")]
            pub id: i64,
            #[tank(name = "region code")]
            pub region: Cow<'static, str>,
            #[tank(name = "gross amount")]
            pub gross: f64,
            pub discount: f64,
            pub fee: f64,
            pub flags: i64,
            pub priority: i32,
            pub channel: i32,
            pub samples: Vec<i64>,
            pub sku: String,
            pub currency: Cow<'static, str>,
            #[tank(name = "is cancelled")]
            pub cancelled: bool,
            #[tank(name = "recorded at")]
            pub recorded_at: i64,
        }

        #[derive(Entity)]
        #[tank(schema = "warehouse", name = "telemetry rollups")]
        struct TelemetryRollup {
            pub region_code: String,
            pub tier_label: String,
            pub bucket_id: i64,
            pub order_count: i64,
            pub net_volume: f64,
            pub avg_fee_sq: f64,
            pub peak_weighted: f64,
        }

        let mut query = Sql()
            .insert_into(TelemetryRollup::table())
            .columns([
                "region_code",
                "tier_label",
                "bucket_id",
                "order_count",
                "net_volume",
                "avg_fee_sq",
                "peak_weighted",
            ])
            .select(cols!(
                Telemetry::region,
                (if Telemetry::priority >= 8 {
                    "critical"
                } else if Telemetry::priority >= 4 {
                    "elevated"
                } else {
                    "normal"
                }) as tier,
                CAST((((Telemetry::flags >> 2) | (Telemetry::samples[0] << 1)) % 16) as i64)
                    as bucket,
                COUNT(*),
                SUM(ABS(Telemetry::gross - Telemetry::discount)) as net_volume,
                AVG(ROUND(POW(Telemetry::fee, 2))),
                MAX(match Telemetry::channel {
                    1 => Telemetry::gross * 2.0,
                    2 | 3 => Telemetry::gross + Telemetry::fee,
                    _ => 0.0,
                }) as peak_weighted,
            ))
            .distinct()
            .from(Telemetry::table())
            .where_expr(expr!(
                Telemetry::cancelled == false
                    && Telemetry::gross != NULL
                    && (Telemetry::sku == "PRO_[A]%" as LIKE
                        || Telemetry::region != "TEST_%" as LIKE)
                    && Telemetry::currency == ("USD", "EUR", "GBP") as IN
                    && ((Telemetry::flags << 1) & 15) > ?
                    && Telemetry::samples[1] + -Telemetry::discount >= ?
                    && Telemetry::recorded_at <= current_timestamp_ms!()
            ))
            .group_by([Telemetry::region, Telemetry::currency])
            .having(expr!(
                COUNT(*) >= 3
                    && SUM(ABS(Telemetry::gross - Telemetry::discount)) > 500.0
                    && AVG(ROUND(POW(Telemetry::fee, 2))) < 250.0
                    && Telemetry::region != "SANDBOX%" as LIKE
                    && Telemetry::currency != ?
                    && peak_weighted - net_volume < ?
            ))
            .order_by(cols!(
                SUM(ABS(Telemetry::gross - Telemetry::discount)) DESC,
                (if Telemetry::priority >= 8 {
                    "critical"
                } else if Telemetry::priority >= 4 {
                    "elevated"
                } else {
                    "normal"
                }) ASC,
                COUNT(*) DESC,
                Telemetry::region ASC,
            ))
            .limit(Some(25))
            .offset(Some(10))
            .build(&DRIVER);

        let Some(Payload::Aggregate(AggregatePayload {
            table, pipeline, ..
        })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected aggregate pipeline for INSERT ... SELECT DISTINCT");
        };
        assert_eq!(table.schema, "analytics");
        assert_eq!(table.name, "raw telemetry");
        assert_eq!(
            *pipeline,
            [
                doc! {
                    "$match": {
                        "$and": [
                            { "is cancelled": false },
                            { "gross amount": { "$ne": Bson::Null } },
                            {
                                "$or": [
                                    {
                                        "sku": {
                                            "$regex": Bson::RegularExpression(Regex {
                                                pattern: r"^PRO.\[A\].*$".into(),
                                                options: Default::default(),
                                            })
                                        }
                                    },
                                    {
                                        "region code": {
                                            "$not": {
                                                "$regex": Bson::RegularExpression(Regex {
                                                    pattern: "^TEST..*$".into(),
                                                    options: Default::default(),
                                                })
                                            },
                                            "$ne": Bson::Null,
                                        }
                                    },
                                ]
                            },
                            { "currency": { "$in": ["USD", "EUR", "GBP"] } },
                            {
                                "$expr": {
                                    "$gt": [
                                        {
                                            "$bitAnd": [
                                                {
                                                    "$multiply": [
                                                        "$flags",
                                                        { "$pow": [Bson::Int64(2), Bson::Int64(1)] },
                                                    ]
                                                },
                                                Bson::Int64(15),
                                            ]
                                        },
                                        "$$param_0",
                                    ]
                                }
                            },
                            {
                                "$expr": {
                                    "$gte": [
                                        {
                                            "$add": [
                                                { "$arrayElemAt": ["$samples", Bson::Int64(1)] },
                                                { "$multiply": [Bson::Int64(-1), "$discount"] },
                                            ]
                                        },
                                        "$$param_1",
                                    ]
                                }
                            },
                            {
                                "$expr": {
                                    "$lte": [
                                        "$recorded at",
                                        { "$toLong": "$$NOW" },
                                    ]
                                }
                            },
                        ]
                    }
                },
                doc! {
                    "$group": {
                        "_id": {
                            "region code": "$region code",
                            "tier": {
                                "$switch": {
                                    "branches": [
                                        {
                                            "case": { "$gte": ["$priority", Bson::Int64(8)] },
                                            "then": "critical",
                                        },
                                        {
                                            "case": { "$gte": ["$priority", Bson::Int64(4)] },
                                            "then": "elevated",
                                        },
                                    ],
                                    "default": "normal",
                                }
                            },
                            "bucket": {
                                "$mod": [
                                    {
                                        "$bitOr": [
                                            {
                                                "$floor": {
                                                    "$divide": [
                                                        "$flags",
                                                        { "$pow": [Bson::Int64(2), Bson::Int64(2)] },
                                                    ]
                                                }
                                            },
                                            {
                                                "$multiply": [
                                                    { "$arrayElemAt": ["$samples", Bson::Int64(0)] },
                                                    { "$pow": [Bson::Int64(2), Bson::Int64(1)] },
                                                ]
                                            },
                                        ]
                                    },
                                    Bson::Int64(16),
                                ]
                            },
                            "currency": "$currency",
                        },
                        "COUNT(*)": { "$sum": Bson::Int64(1) },
                        "net_volume": {
                            "$sum": {
                                "$abs": {
                                    "$subtract": ["$gross amount", "$discount"],
                                }
                            }
                        },
                        "AVG(ROUND(POW(fee,2)))": {
                            "$avg": {
                                "$round": {
                                    "$pow": ["$fee", Bson::Int64(2)],
                                }
                            }
                        },
                        "peak_weighted": {
                            "$max": {
                                "$switch": {
                                    "branches": [
                                        {
                                            "case": { "$eq": ["$channel", Bson::Int64(1)] },
                                            "then": { "$multiply": ["$gross amount", 2.0] },
                                        },
                                        {
                                            "case": { "$eq": ["$channel", Bson::Int64(2)] },
                                            "then": { "$add": ["$gross amount", "$fee"] },
                                        },
                                        {
                                            "case": { "$eq": ["$channel", Bson::Int64(3)] },
                                            "then": { "$add": ["$gross amount", "$fee"] },
                                        },
                                    ],
                                    "default": 0.0,
                                }
                            }
                        },
                    }
                },
                doc! {
                    "$match": {
                        "$and": [
                            { "COUNT(*)": { "$gte": Bson::Int64(3) } },
                            { "net_volume": { "$gt": 500.0 } },
                            { "AVG(ROUND(POW(fee,2)))": { "$lt": 250.0 } },
                            {
                                "_id.region code": {
                                    "$not": {
                                        "$regex": Bson::RegularExpression(Regex {
                                            pattern: "^SANDBOX.*$".into(),
                                            options: Default::default(),
                                        })
                                    },
                                    "$ne": Bson::Null,
                                }
                            },
                            {
                                "$expr": {
                                    "$ne": ["$_id.currency", "$$param_2"],
                                }
                            },
                            {
                                "$expr": {
                                    "$lt": [
                                        { "$subtract": ["$peak_weighted", "$net_volume"] },
                                        "$$param_3",
                                    ]
                                }
                            },
                        ]
                    }
                },
                doc! {
                    "$sort": {
                        "net_volume": -1,
                        "_id.tier": 1,
                        "COUNT(*)": -1,
                        "_id.region code": 1,
                    }
                },
                doc! { "$skip": Bson::Int64(10) },
                doc! { "$limit": 25 },
                doc! {
                    "$project": {
                        "region_code": "$_id.region code",
                        "tier_label": "$_id.tier",
                        "bucket_id": "$_id.bucket",
                        "order_count": "$COUNT(*)",
                        "net_volume": "$net_volume",
                        "avg_fee_sq": "$AVG(ROUND(POW(fee,2)))",
                        "peak_weighted": "$peak_weighted",
                    }
                },
                doc! {
                    "$merge": {
                        "into": { "db": "warehouse", "coll": "telemetry rollups" },
                        "whenMatched": "fail",
                        "whenNotMatched": "insert",
                    }
                },
            ]
        );
    }
}
