#![allow(unused_imports)]
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::pin::pin;
use std::{str::FromStr, sync::Arc, sync::LazyLock};
use tank::QueryBuilder;
use tank::{
    AsValue, Dataset, Entity, Executor, FixedDecimal, cols, expr, join,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, PrimitiveDateTime, Time};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Default, Entity, Debug)]
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

#[derive(Entity, Debug)]
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

#[derive(Entity, Debug)]
#[tank(schema = "shopping", primary_key = (user, product))]
struct Cart {
    #[tank(references = User::id)]
    user: Uuid,
    #[tank(references = Product::id)]
    product: usize,
    /// The price can stay locked once added to the shopping cart
    price: FixedDecimal<8, 2>,
    timestamp: PrimitiveDateTime,
}

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
            price: Decimal::new(12_99, 2).into(),
            desc: Some("Keeps your coffee warm and your compiler calm.".into()),
            stock: 42.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["kitchen".into(), "coffee".into(), "metal".into()].into(),
        },
        Product {
            id: 2,
            name: "Zero-Cost Abstraction Hoodie".into(),
            price: Decimal::new(49_95, 2).into(),
            desc: Some("For developers who think runtime overhead is a moral failure.".into()),
            stock: 10.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["clothing".into(), "nerdwear".into()].into(),
        },
        Product {
            id: 3,
            name: "Thread-Safe Notebook".into(),
            price: Decimal::new(7_50, 2).into(),
            desc: None,
            stock: 0.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["stationery".into()].into(),
        },
        Product {
            id: 4,
            name: "Async Teapot".into(),
            price: Decimal::new(25_00, 2).into(),
            desc: Some("Returns 418 on brew() call.".into()),
            stock: 3.into(),
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["kitchen".into(), "humor".into()].into(),
        },
    ];
    Product::insert_many(executor, &products)
        .await
        .expect("Could not insert the products");
    let total_products = Product::find_many(executor, true, None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(total_products, 4);
    let ordered_products = executor
        .fetch(
            QueryBuilder::new()
                .select([Product::id, Product::name, Product::price])
                .from(Product::table())
                .where_expr(expr!(Product::stock > 0))
                .order_by(cols!(Product::price ASC))
                .build(&executor.driver()),
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
    let zero_stock = Product::find_one(executor, expr!(Product::stock == 0))
        .await
        .expect("Failed to query product with zero stock")
        .expect("Expected a product with zero stock");
    assert_eq!(zero_stock.id, 3);

    // Decrease stock for product id 4 by 1 and verify save works
    let mut prod4 = Product::find_one(executor, expr!(Product::id == 4))
        .await
        .expect("Failed to query product 4")
        .expect("Product 4 expected");
    let old_stock = prod4.stock.unwrap_or(0);
    prod4.stock = Some(old_stock - 1);
    prod4
        .save(executor)
        .await
        .expect("Failed to save updated product 4");
    let prod4_after = Product::find_one(executor, expr!(Product::id == 4))
        .await
        .expect("Failed to query product 4 after update")
        .expect("Product 4 expected after update");
    assert_eq!(prod4_after.stock, Some(old_stock - 1));

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
    let row = pin!(
        executor.fetch(
            QueryBuilder::new()
                .select(cols!(COUNT(*)))
                .from(User::table())
                .where_expr(true)
                .limit(Some(1))
                .build(&executor.driver())
        )
    )
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
            price: Decimal::new(12_99, 2).into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 1).unwrap(),
                Time::from_hms(9, 0, 0).unwrap(),
            ),
        },
        Cart {
            user: users[0].id,
            product: 2,
            price: Decimal::new(49_95, 2).into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 1).unwrap(),
                Time::from_hms(9, 5, 0).unwrap(),
            ),
        },
        Cart {
            user: users[1].id,
            product: 4,
            price: Decimal::new(23_50, 2).into(),
            timestamp: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::March, 3).unwrap(),
                Time::from_hms(14, 12, 0).unwrap(),
            ),
        },
    ];
    Cart::insert_many(executor, &carts)
        .await
        .expect("Could not insert the carts");
    let cart_count = Cart::find_many(executor, true, None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(cart_count, 3);

    // Product 4 in cart has different price than current product price
    let cart_for_4 = Cart::find_one(executor, expr!(Cart::product == 4))
        .await
        .expect("Failed to query cart for product 4");
    let cart_for_4 = cart_for_4.expect("Expected a cart for product 4");
    let product4 = Product::find_one(executor, expr!(Product::id == 4))
        .await
        .expect("Failed to query product 4 for price check")
        .expect("Expected product 4");
    assert_eq!(cart_for_4.price.0, Decimal::new(23_50, 2));
    assert_eq!(product4.price.0, Decimal::new(25_00, 2));

    // Delete the cart containing product 2
    let cart_for_2 = Cart::find_one(executor, expr!(Cart::product == 2))
        .await
        .expect("Failed to query cart for product 2")
        .expect("Expected a cart for product 2");
    cart_for_2
        .delete(executor)
        .await
        .expect("Failed to delete cart for product 2");
    let cart_count_after = Cart::find_many(executor, true, None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(cart_count_after, 2);

    #[cfg(not(feature = "disable-joins"))]
    {
        #[derive(Entity, PartialEq, Debug)]
        struct Carts {
            user: String,
            product: String,
            price: Decimal,
        }
        let carts: Vec<Carts> = executor
            .fetch(
                QueryBuilder::new()
                    .select(cols!(
                        Product::name as product,
                        User::name as user,
                        Cart::price
                    ))
                    .from(join!(
                        User INNER JOIN Cart ON User::id == Cart::user
                            JOIN Product ON Cart::product == Product::id
                    ))
                    .where_expr(true)
                    .order_by(cols!(Product::name ASC, User::name ASC))
                    .build(&executor.driver()),
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
                    price: Decimal::new(23_50, 2),
                },
                Carts {
                    user: "Alice Compiler".into(),
                    product: "Rust-Proof Coffee Mug".into(),
                    price: Decimal::new(12_99, 2),
                },
            ]
        )
    }
}
