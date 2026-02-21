#![allow(unused_imports)]
use std::{future, sync::LazyLock};
use tank::{
    Entity, Executor, QueryBuilder, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, macros::date};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity)]
#[tank(primary_key = (name, country, value, date))]
pub struct Metric {
    person: String,
    country: String,
    #[tank(clustering_key)]
    date: Date,
    name: String,
    #[tank(clustering_key)]
    value: f64,
}

#[derive(Entity)]
struct MetricValue {
    pub value: f64,
}

pub async fn metrics<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Metric::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Metric table");
    Metric::create_table(executor, true, true)
        .await
        .expect("Failed to create Metric table");

    // Insert
    let values = vec![
        // Alice, IT (female)
        Metric {
            person: "alice".into(),
            country: "IT".into(),
            date: date!(2024 - 1 - 1),
            name: "height_cm".into(),
            value: 165.0,
        },
        Metric {
            person: "alice".into(),
            country: "IT".into(),
            date: date!(2024 - 1 - 1),
            name: "weight_kg".into(),
            value: 62.5,
        },
        Metric {
            person: "alice".into(),
            country: "IT".into(),
            date: date!(2024 - 6 - 1),
            name: "weight_kg".into(),
            value: 64.2,
        },
        Metric {
            person: "alice".into(),
            country: "IT".into(),
            date: date!(2024 - 6 - 1),
            name: "income_eur".into(),
            value: 56000.0,
        },
        Metric {
            person: "alice".into(),
            country: "IT".into(),
            date: date!(2025 - 4 - 1),
            name: "income_eur".into(),
            value: 68000.0,
        },
        // Bob, NL (male)
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2024 - 1 - 1),
            name: "height_cm".into(),
            value: 188.0,
        },
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2024 - 1 - 1),
            name: "weight_kg".into(),
            value: 81.0,
        },
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2025 - 6 - 12),
            name: "weight_kg".into(),
            value: 82.5,
        },
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2024 - 12 - 1),
            name: "income_eur".into(),
            value: 88000.0,
        },
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2025 - 3 - 2),
            name: "income_eur".into(),
            value: 120000.0,
        },
        Metric {
            person: "bob".into(),
            country: "NL".into(),
            date: date!(2026 - 2 - 2),
            name: "income_eur".into(),
            value: 130000.0,
        },
        // Clara, DE (female)
        Metric {
            person: "clara".into(),
            country: "DE".into(),
            date: date!(2024 - 2 - 1),
            name: "height_cm".into(),
            value: 170.0,
        },
        Metric {
            person: "clara".into(),
            country: "DE".into(),
            date: date!(2024 - 2 - 1),
            name: "weight_kg".into(),
            value: 60.0,
        },
        Metric {
            person: "clara".into(),
            country: "DE".into(),
            date: date!(2025 - 1 - 1),
            name: "income_eur".into(),
            value: 72000.0,
        },
        // David, UK (male)
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2024 - 3 - 10),
            name: "height_cm".into(),
            value: 182.0,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2024 - 3 - 10),
            name: "weight_kg".into(),
            value: 88.5,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2024 - 3 - 10),
            name: "weight_kg".into(),
            value: 86.0,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2024 - 11 - 1),
            name: "income_eur".into(),
            value: 69000.0,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2025 - 11 - 1),
            name: "income_eur".into(),
            value: 102000.0,
        },
        // Eva, ES (female)
        Metric {
            person: "eva".into(),
            country: "ES".into(),
            date: date!(2024 - 4 - 5),
            name: "height_cm".into(),
            value: 162.0,
        },
        Metric {
            person: "eva".into(),
            country: "ES".into(),
            date: date!(2024 - 4 - 5),
            name: "weight_kg".into(),
            value: 58.0,
        },
        Metric {
            person: "eva".into(),
            country: "ES".into(),
            date: date!(2024 - 12 - 1),
            name: "income_eur".into(),
            value: 42000.0,
        },
        Metric {
            person: "eva".into(),
            country: "ES".into(),
            date: date!(2025 - 12 - 1),
            name: "income_eur".into(),
            value: 47000.0,
        },
        // Marco, IT (male)
        Metric {
            person: "marco".into(),
            country: "IT".into(),
            date: date!(2024 - 1 - 15),
            name: "height_cm".into(),
            value: 178.0,
        },
        Metric {
            person: "marco".into(),
            country: "IT".into(),
            date: date!(2024 - 1 - 15),
            name: "weight_kg".into(),
            value: 78.0,
        },
        Metric {
            person: "marco".into(),
            country: "IT".into(),
            date: date!(2024 - 10 - 1),
            name: "weight_kg".into(),
            value: 80.5,
        },
        Metric {
            person: "marco".into(),
            country: "IT".into(),
            date: date!(2024 - 10 - 1),
            name: "income_eur".into(),
            value: 61000.0,
        },
        Metric {
            person: "marco".into(),
            country: "IT".into(),
            date: date!(2025 - 10 - 1),
            name: "income_eur".into(),
            value: 72000.0,
        },
        // Sophie, UK (female)
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2024 - 2 - 20),
            name: "height_cm".into(),
            value: 168.0,
        },
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2024 - 2 - 20),
            name: "weight_kg".into(),
            value: 61.0,
        },
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2024 - 12 - 1),
            name: "income_eur".into(),
            value: 58000.0,
        },
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2024 - 12 - 2),
            name: "weight_kg".into(),
            value: 67.5,
        },
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2025 - 12 - 1),
            name: "income_eur".into(),
            value: 66000.0,
        },
        // Partition isolation sanity check
        Metric {
            person: "alice".into(),
            country: "CA".into(),
            date: date!(2024 - 6 - 1),
            name: "income_eur".into(),
            value: 77000.0,
        },
    ];
    /*
    person,country,date,name,value
    alice,IT,2024-01-01,height_cm,165.0
    bob,NL,2024-01-01,height_cm,188.0
    clara,DE,2024-02-01,height_cm,170.0
    david,UK,2024-03-10,height_cm,182.0
    eva,ES,2024-04-05,height_cm,162.0
    marco,IT,2024-01-15,height_cm,178.0
    sophie,UK,2024-02-20,height_cm,168.0
    alice,IT,2024-06-01,income_eur,56000.0
    alice,IT,2025-04-01,income_eur,68000.0
    bob,NL,2024-12-01,income_eur,88000.0
    bob,NL,2025-03-02,income_eur,120000.0
    bob,NL,2026-02-02,income_eur,130000.0
    clara,DE,2025-01-01,income_eur,72000.0
    david,UK,2024-11-01,income_eur,69000.0
    david,UK,2025-11-01,income_eur,102000.0
    eva,ES,2024-12-01,income_eur,42000.0
    eva,ES,2025-12-01,income_eur,47000.0
    marco,IT,2024-10-01,income_eur,61000.0
    marco,IT,2025-10-01,income_eur,72000.0
    sophie,UK,2024-12-01,income_eur,58000.0
    sophie,UK,2025-12-01,income_eur,66000.0
    alice,CA,2024-06-01,income_eur,77000.0
    alice,IT,2024-01-01,weight_kg,62.5
    bob,NL,2024-01-01,weight_kg,81.0
    bob,NL,2025-06-12,weight_kg,82.5
    clara,DE,2024-02-01,weight_kg,60.0
    david,UK,2024-03-10,weight_kg,88.5
    david,UK,2024-03-10,weight_kg,86.0
    eva,ES,2024-04-05,weight_kg,58.0
    marco,IT,2024-01-15,weight_kg,78.0
    marco,IT,2024-10-01,weight_kg,80.5
    sophie,UK,2024-02-20,weight_kg,61.0
    sophie,UK,2024-12-02,weight_kg,67.5
    */

    Metric::insert_many(executor, &values)
        .await
        .expect("Could not insert the entities");

    let height = 170;
    let heights = executor
        .fetch(
            QueryBuilder::new()
                .select([Metric::value, Metric::date])
                .from(Metric::table())
                .where_expr(expr!(
                    Metric::name == "height_cm"
                        && Metric::country == "IT"
                        && Metric::value >= #height
                ))
                .order_by(cols!(Metric::value DESC, Metric::date DESC))
                .build(&executor.driver()),
        )
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Coult not get the Italy height values");
    assert_eq!(heights, [178.0]);

    // Incomes in Italy
    let italy_incomes = executor
        .fetch(
            QueryBuilder::new()
                .select([Metric::value])
                .from(Metric::table())
                .where_expr(expr!(
                    Metric::name == "income_eur" && Metric::country == "IT"
                ))
                .order_by(cols!(Metric::value ASC))
                .build(&executor.driver()),
        )
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get IT incomes");
    assert_eq!(italy_incomes, [56000.0, 61000.0, 68000.0, 72000.0]);

    // Highest income in the UK
    let latest_income = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MAX(Metric::value) as value))
                .from(Metric::table())
                .where_expr(expr!(
                    Metric::name == "income_eur" && Metric::country == "UK"
                ))
                .build(&executor.driver()),
        )
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get latest UK income");
    assert_eq!(latest_income, [102000.0]);

    // Prepared queries
    let mut prepared = executor
        .prepare(
            QueryBuilder::new()
                .select([Metric::value, Metric::date])
                .from(Metric::table())
                .where_expr(expr!(Metric::country == ? && Metric::name == ?))
                .order_by(cols!(Metric::value DESC, Metric::date DESC))
                .build(&executor.driver()),
        )
        .await
        .expect("Failed to prepare metric query");

    prepared.bind("ES").unwrap().bind("height_cm").unwrap();
    let spain_heights = executor
        .fetch(&mut prepared)
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not fetch sophie heights");
    assert_eq!(spain_heights, [162.0]);

    prepared.bind("NL").unwrap().bind("weight_kg").unwrap();
    let netherlands_weights = executor
        .fetch(&mut prepared)
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not fetch sophie heights");
    assert_eq!(netherlands_weights, [82.5, 81.0]);

    prepared.bind("IT").unwrap().bind("weight_kg").unwrap();
    let italy_weights = executor
        .fetch(&mut prepared)
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not fetch sophie heights");
    assert_eq!(italy_weights, [80.5, 78.0, 64.2, 62.5]);

    #[cfg(not(feature = "disable-groups"))]
    {
        #[derive(Entity, PartialEq, Debug)]
        struct AverageMetrics {
            name: String,
            country: String,
            avg: f32,
        }
        let averages = executor
            .fetch(
                QueryBuilder::new()
                    .select(cols!(
                        Metric::name,
                        Metric::country,
                        AVG(Metric::value) as avg,
                    ))
                    .from(Metric::table())
                    .group_by([Metric::name, Metric::country])
                    .order_by(cols!(Metric::name ASC, avg DESC))
                    .build(&executor.driver()),
            )
            .map_ok(AverageMetrics::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not get the products ordered by increasing price");
        assert_eq!(
            averages,
            [
                AverageMetrics {
                    name: "height_cm".into(),
                    country: "NL".into(),
                    avg: 188.0,
                },
                AverageMetrics {
                    name: "height_cm".into(),
                    country: "UK".into(),
                    avg: 175.0,
                },
                AverageMetrics {
                    name: "height_cm".into(),
                    country: "IT".into(),
                    avg: 171.5,
                },
                AverageMetrics {
                    name: "height_cm".into(),
                    country: "DE".into(),
                    avg: 170.0,
                },
                AverageMetrics {
                    name: "height_cm".into(),
                    country: "ES".into(),
                    avg: 162.0,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "NL".into(),
                    avg: 112666.664,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "CA".into(),
                    avg: 77000.0,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "UK".into(),
                    avg: 73750.0,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "DE".into(),
                    avg: 72000.0,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "IT".into(),
                    avg: 64250.0,
                },
                AverageMetrics {
                    name: "income_eur".into(),
                    country: "ES".into(),
                    avg: 44500.0,
                },
                AverageMetrics {
                    name: "weight_kg".into(),
                    country: "NL".into(),
                    avg: 81.75,
                },
                AverageMetrics {
                    name: "weight_kg".into(),
                    country: "UK".into(),
                    avg: 75.75,
                },
                AverageMetrics {
                    name: "weight_kg".into(),
                    country: "IT".into(),
                    avg: 71.3,
                },
                AverageMetrics {
                    name: "weight_kg".into(),
                    country: "DE".into(),
                    avg: 60.0,
                },
                AverageMetrics {
                    name: "weight_kg".into(),
                    country: "ES".into(),
                    avg: 58.0,
                }
            ]
        );
    }

    #[cfg(not(feature = "disable-groups"))]
    {
        #[derive(Entity, PartialEq, Debug)]
        struct CountryMaxIncome {
            country: String,
            max_income: f64,
        }

        let max_incomes = executor
            .fetch(
                QueryBuilder::new()
                    .select(cols!(Metric::country, MAX(value) as max_income,))
                    .from(Metric::table())
                    .where_expr(expr!(name == "income_eur"))
                    .group_by([Metric::country])
                    .order_by(cols!(max_income DESC, Metric::country ASC))
                    .build(&executor.driver()),
            )
            .map_ok(CountryMaxIncome::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not get max incomes per country");

        assert_eq!(
            max_incomes,
            [
                CountryMaxIncome {
                    country: "NL".into(),
                    max_income: 130000.0,
                },
                CountryMaxIncome {
                    country: "UK".into(),
                    max_income: 102000.0,
                },
                CountryMaxIncome {
                    country: "CA".into(),
                    max_income: 77000.0,
                },
                CountryMaxIncome {
                    country: "DE".into(),
                    max_income: 72000.0,
                },
                CountryMaxIncome {
                    country: "IT".into(),
                    max_income: 72000.0,
                },
                CountryMaxIncome {
                    country: "ES".into(),
                    max_income: 47000.0,
                },
            ]
        );
    }

    #[cfg(not(feature = "disable-groups"))]
    {
        #[derive(Entity, PartialEq, Debug)]
        struct MetricGlobalStats {
            name: String,
            min_val: f64,
            max_val: f64,
            total_val: f64,
        }

        let global_stats = executor
            .fetch(
                QueryBuilder::new()
                    .select(cols!(
                        Metric::name,
                        MIN(Metric::value) as min_val,
                        MAX(Metric::value) as max_val,
                        SUM(Metric::value) as total_val,
                    ))
                    .from(Metric::table())
                    .group_by([Metric::name])
                    .order_by(cols!(name ASC))
                    .build(&executor.driver()),
            )
            .map_ok(MetricGlobalStats::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not get global metric stats");

        assert_eq!(
            global_stats,
            [
                MetricGlobalStats {
                    name: "height_cm".into(),
                    min_val: 162.0,
                    max_val: 188.0,
                    total_val: 1213.0,
                },
                MetricGlobalStats {
                    name: "income_eur".into(),
                    min_val: 42000.0,
                    max_val: 130000.0,
                    total_val: 1128000.0,
                },
                MetricGlobalStats {
                    name: "weight_kg".into(),
                    min_val: 58.0,
                    max_val: 88.5,
                    total_val: 869.7, // Note: watch out for f64 epsilon precision issues here depending on the DB engine!
                },
            ]
        );
    }
}
