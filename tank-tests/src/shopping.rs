#![allow(unused_imports)]
use rust_decimal::Decimal;
use std::pin::pin;
use std::{str::FromStr, sync::Arc, sync::LazyLock};
use tank::{
    AsValue, DataSet, Entity, Executor, FixedDecimal, cols, expr, join,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, PrimitiveDateTime, Time};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Default, Debug, Entity)]
#[tank(schema = "shopping", primary_key = Self::id)]
struct Product {
    id: usize,
    name: String,
    price: FixedDecimal<8, 2>,
    desc: Option<String>,
    stock: Option<isize>,
    #[cfg(not(feature = "disable-lists"))]
    tags: Vec<String>,
}

#[derive(Debug, Entity)]
#[tank(schema = "shopping", primary_key = Self::id)]
struct User {
    id: Uuid,
    name: String,
    email: String,
    birthday: Date,
    #[cfg(not(feature = "disable-lists"))]
    preferences: Option<Arc<Vec<String>>>,
    registered: PrimitiveDateTime,
}

#[derive(Debug, Entity)]
#[tank(schema = "shopping")]
struct Cart {
    #[tank(references = User::id)]
    user: Uuid,
    #[tank(references = Product::id)]
    product: usize,
    /// The price can stay locked once added to the shopping cart
    price: FixedDecimal<8, 2>,
    timestamp: PrimitiveDateTime,
}

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn shopping<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Product
    Product::drop_table(executor, true, false)
        .await
        .expect("Failed to drop product table");
    Product::create_table(executor, false, true)
        .await
        .expect("Failed to create the product table");
    let products = [
        Product {
            id: 1,
            name: "Rust-Proof Coffee Mug".into(),
            price: Decimal::from_str("12.99").unwrap().into(),
            desc: Some("Keeps your coffee warm and your compiler calm.".into()),
            stock: 42.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["kitchen".into(), "coffee".into(), "metal".into()].into(),
        },
        Product {
            id: 2,
            name: "Zero-Cost Abstraction Hoodie".into(),
            price: Decimal::from_str("49.95").unwrap().into(),
            desc: Some("For developers who think runtime overhead is a moral failure.".into()),
            stock: 10.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["clothing".into(), "nerdwear".into()].into(),
        },
        Product {
            id: 3,
            name: "Thread-Safe Notebook".into(),
            price: Decimal::from_str("7.50").unwrap().into(),
            desc: None,
            stock: 0.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["stationery".into()].into(),
        },
        Product {
            id: 4,
            name: "Async Teapot".into(),
            price: Decimal::from_str("25.00").unwrap().into(),
            desc: Some("Returns 418 on brew() call.".into()),
            stock: 3.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["kitchen".into(), "humor".into()].into(),
        },
    ];
    Product::insert_many(executor, &products)
        .await
        .expect("Could not insert the products");
    let ordered_products = Product::table()
        .select(
            executor,
            cols!(Product::id, Product::name, Product::price ASC),
            &expr!(Product::stock > 0),
            None,
        )
        .map(|r| r.and_then(Product::from_row))
        .try_collect::<Vec<Product>>()
        .await
        .expect("Could not get the products ordered by increasing price");
    assert!(
        ordered_products.iter().map(|v| &v.name).eq([
            "Rust-Proof Coffee Mug",
            "Async Teapot",
            "Zero-Cost Abstraction Hoodie"
        ]
        .into_iter())
    );

    // User
    User::drop_table(executor, true, false)
        .await
        .expect("Failed to drop user table");
    User::create_table(executor, false, false)
        .await
        .expect("Failed to create the user table");
    let users = vec![
        User {
            id: Uuid::new_v4(),
            name: "Alice Compiler".into(),
            email: "alice@example.com".into(),
            birthday: Date::from_calendar_date(1995, Month::May, 17).unwrap(),
            #[cfg(not(feature = "disable-lists"))]
            preferences: Some(vec!["dark_mode".into(), "express_shipping".into()].into()),
            registered: PrimitiveDateTime::new(
                Date::from_calendar_date(2023, Month::January, 2).unwrap(),
                Time::from_hms(10, 30, 0).unwrap(),
            ),
        },
        User {
            id: Uuid::new_v4(),
            name: "Bob Segfault".into(),
            email: "bob@crashmail.net".into(),
            birthday: Date::from_calendar_date(1988, Month::March, 12).unwrap(),
            #[cfg(not(feature = "disable-lists"))]
            preferences: None,
            registered: PrimitiveDateTime::new(
                Date::from_calendar_date(2024, Month::June, 8).unwrap(),
                Time::from_hms(22, 15, 0).unwrap(),
            ),
        },
    ];
    User::insert_many(executor, &users)
        .await
        .expect("Could not insert the users");
    let row = pin!(User::table().select(executor, cols!(COUNT(*)), &true, Some(1)))
        .try_next()
        .await
        .expect("Failed to query for count")
        .expect("Did not return some value");
    assert_eq!(i64::try_from_value(row.values[0].clone()).unwrap(), 2);

    // Cart
    Cart::drop_table(executor, true, false)
        .await
        .expect("Failed to drop cart table");
    Cart::create_table(executor, false, false)
        .await
        .expect("Failed to create the cart table");
    let carts = vec![
        Cart {
            user: users[0].id,
            product: 1,
            price: Decimal::from_str("12.99").unwrap().into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 1).unwrap(),
                Time::from_hms(9, 0, 0).unwrap(),
            ),
        },
        Cart {
            user: users[0].id,
            product: 2,
            price: Decimal::from_str("49.95").unwrap().into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 1).unwrap(),
                Time::from_hms(9, 5, 0).unwrap(),
            ),
        },
        Cart {
            user: users[1].id,
            product: 4,
            price: Decimal::from_str("23.50").unwrap().into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 3).unwrap(),
                Time::from_hms(14, 12, 0).unwrap(),
            ),
        },
    ];
    Cart::insert_many(executor, &carts)
        .await
        .expect("Could not insert the carts");

    // Join
    #[derive(Debug, Entity, PartialEq)]
    struct Carts {
        user: String,
        product: String,
        price: Decimal,
    }
    let carts: Vec<Carts> = join!(
        User INNER JOIN Cart ON User::id == Cart::user
            JOIN Product ON Cart::product == Product::id
    )
    .select(
        executor,
        cols!(Product::name as product ASC, User::name as user ASC, Cart::price),
        &true,
        None,
    )
    .map_ok(Carts::from_row)
    .map(Result::flatten)
    .try_collect::<Vec<_>>()
    .await
    .expect("Could not get the products ordered by increasing price");
    assert_eq!(
        carts,
        &[
            Carts {
                user: "Bob Segfault".into(),
                product: "Async Teapot".into(),
                price: Decimal::from_str("23.50").unwrap(),
            },
            Carts {
                user: "Alice Compiler".into(),
                product: "Rust-Proof Coffee Mug".into(),
                price: Decimal::from_str("12.99").unwrap(),
            },
            Carts {
                user: "Alice Compiler".into(),
                product: "Zero-Cost Abstraction Hoodie".into(),
                price: Decimal::from_str("49.95").unwrap(),
            },
        ]
    )
}
