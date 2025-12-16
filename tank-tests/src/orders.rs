use rust_decimal::Decimal;
use std::sync::LazyLock;
use tank::{DataSet, Entity, Executor, FixedDecimal, Passive, cols, expr, stream::TryStreamExt};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone, PartialEq, Eq, Hash)]
#[tank(schema = "testing", name = "orders")]
pub struct Order {
    #[tank(primary_key)]
    pub id: Passive<Uuid>,
    pub customer_id: Uuid,
    pub country: String,
    pub total: FixedDecimal<16, 2>,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn orders<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Order::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Order table");
    Order::create_table(executor, false, true)
        .await
        .expect("Failed to create Order table");

    // Data
    let orders = vec![
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            country: "Germany".into(),
            total: Decimal::new(12999, 2).into(),
            status: "paid".into(),
            created_at: chrono::Utc::now() - chrono::Duration::days(5),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
            country: "Italy".into(),
            total: Decimal::new(8990, 2).into(),
            status: "shipped".into(),
            created_at: chrono::Utc::now() - chrono::Duration::hours(3),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            country: "Germany".into(),
            total: Decimal::new(45900, 2).into(),
            status: "shipped".into(),
            created_at: chrono::Utc::now() - chrono::Duration::days(9),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            country: "Spain".into(),
            total: Decimal::new(22950, 2).into(),
            status: "paid".into(),
            created_at: chrono::Utc::now() - chrono::Duration::days(1),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
            country: "Germany".into(),
            total: Decimal::new(50, 2).into(),
            status: "paid".into(),
            created_at: chrono::Utc::now() - chrono::Duration::days(30),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
            country: "Germany".into(),
            total: Decimal::new(111899, 2).into(),
            status: "shipped".into(),
            created_at: chrono::Utc::now() - chrono::Duration::days(30),
        },
        Order {
            id: Uuid::new_v4().into(),
            customer_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
            country: "Italy".into(),
            total: Decimal::new(4445, 2).into(),
            status: "paid".into(),
            created_at: chrono::Utc::now() - chrono::Duration::hours(2),
        },
    ];
    let result = Order::insert_many(executor, orders.iter())
        .await
        .expect("Failed to insert orders");
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 7);
    }

    // Prepare
    let mut query = Order::table()
        .prepare(
            executor,
            cols!(
                Order::id,
                Order::customer_id,
                Order::country,
                Order::total DESC,
                Order::status,
                Order::created_at
            ),
            &expr!(
                Order::status == (?, ?) as IN &&
                Order::created_at >= ? &&
                Order::total >= ? &&
                Order::country == (?, ?) as IN
            ),
            None,
        )
        .await
        .expect("Failed to prepare the query");
    assert!(query.is_prepared(), "Query should be marked as prepared");

    // 100+€ orders from last 10 days from Germany or Spain
    query
        .bind("paid")
        .unwrap()
        .bind("shipped")
        .unwrap()
        .bind(chrono::Utc::now() - chrono::Duration::days(10))
        .unwrap()
        .bind(99.99)
        .unwrap()
        .bind("Germany")
        .unwrap()
        .bind("Spain")
        .unwrap();
    let orders = executor
        .fetch(&mut query)
        .and_then(|v| async { Order::from_row(v) })
        .map_ok(|v| format!("{}, {}, {:.2}€", v.country, v.status, v.total.0))
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not run query 1");
    assert_eq!(
        orders,
        [
            "Germany, shipped, 459.00€".to_string(),
            "Spain, paid, 229.50€".to_string(),
            "Germany, paid, 129.99€".to_string(),
        ]
    );
    assert!(query.is_prepared());

    // All orders above 1€ from Germany or Italy
    query.clear_bindings().expect("Failed to clear bindings");
    query
        .bind("paid")
        .unwrap()
        .bind("shipped")
        .unwrap()
        .bind(chrono::Utc::now() - chrono::Duration::days(365))
        .unwrap()
        .bind(1)
        .unwrap()
        .bind("Germany")
        .unwrap()
        .bind("Italy")
        .unwrap();
    let orders: Vec<String> = executor
        .fetch(&mut query)
        .and_then(|v| async { Order::from_row(v) })
        .map_ok(|v| format!("{}, {}, {:.2}€", v.country, v.status, v.total.0))
        .try_collect()
        .await
        .expect("Could not run query 2");
    assert_eq!(
        orders,
        [
            "Germany, shipped, 1118.99€".to_string(),
            "Germany, shipped, 459.00€".to_string(),
            "Germany, paid, 129.99€".to_string(),
            "Italy, shipped, 89.90€".to_string(),
            "Italy, paid, 44.45€".to_string(),
        ]
    );
    assert!(query.is_prepared());
}
