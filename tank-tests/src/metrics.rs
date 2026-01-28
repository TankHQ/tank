use std::{future, sync::LazyLock};
use tank::{Entity, Executor, QueryBuilder, cols, expr, stream::TryStreamExt};
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
            value: 86.0,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2024 - 11 - 1),
            name: "income_gbp".into(),
            value: 65000.0,
        },
        Metric {
            person: "david".into(),
            country: "UK".into(),
            date: date!(2025 - 11 - 1),
            name: "income_gbp".into(),
            value: 93000.0,
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
            name: "income_gbp".into(),
            value: 52000.0,
        },
        Metric {
            person: "sophie".into(),
            country: "UK".into(),
            date: date!(2025 - 12 - 1),
            name: "income_gbp".into(),
            value: 58000.0,
        },
        // Partition isolation sanity check
        Metric {
            person: "alice".into(),
            country: "CA".into(),
            date: date!(2024 - 6 - 1),
            name: "income_usd".into(),
            value: 69000.0,
        },
    ];

    Metric::insert_many(executor, &values)
        .await
        .expect("Could not insert the entities");

    let height = 170;
    let heights = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(Metric::value DESC, Metric::date DESC))
                .from(Metric::table())
                .where_condition(expr!(
                    Metric::name == "height_cm"
                        && Metric::country == "IT"
                        && Metric::value >= #height
                ))
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
                .select(cols!(Metric::value ASC))
                .from(Metric::table())
                .where_condition(expr!(
                    Metric::name == "income_eur" && Metric::country == "IT"
                ))
                .build(&executor.driver()),
        )
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get alice incomes");
    assert_eq!(italy_incomes, [56000.0, 61000.0, 68000.0, 72000.0]);

    // Highest income in the UK
    let latest_income = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MAX(Metric::value) as value))
                .from(Metric::table())
                .where_condition(expr!(
                    Metric::name == "income_gbp" && Metric::country == "UK"
                ))
                .build(&executor.driver()),
        )
        .and_then(|v| future::ready(MetricValue::from_row(v).map(|v| v.value)))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get latest alice income");
    assert_eq!(latest_income, [93000.0]);

    // Prepared queries
    let mut prepared = executor
        .prepare(
            QueryBuilder::new()
                .select(cols!(Metric::value DESC, Metric::date DESC))
                .from(Metric::table())
                .where_condition(expr!(Metric::country == ? && Metric::name == ?))
                .build(&executor.driver())
                .into(),
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
}
