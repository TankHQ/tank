#![allow(unused_imports)]
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    i128,
    pin::pin,
    sync::{Arc, LazyLock},
};
use tank::{
    Driver, Entity, Executor, FixedDecimal, Query, QueryResult, RawQuery, RowsAffected, SqlWriter,
    stream::TryStreamExt,
};
use time::{Date, Time, macros::date};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Entity, PartialEq)]
pub struct SimpleFields {
    #[tank(primary_key)]
    alpha: u8,
    bravo: Option<i32>,
    charlie: Option<i16>,
    delta: Option<u64>,
    echo: Option<Uuid>,
    #[cfg(not(feature = "disable-large-integers"))]
    foxtrot: Option<i128>,
    golf: Option<Time>,
    hotel: Option<Cow<'static, str>>,
    india: Box<Option<char>>,
    juliet: Option<bool>,
    kilo: Option<u32>,
    lima: Arc<Option<f32>>,
    mike: Option<Date>,
    november: Option<Cell<FixedDecimal<4, 2>>>,
    oscar: Option<RefCell<FixedDecimal<8, 3>>>,
    papa: Option<FixedDecimal<20, 1>>,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn simple<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    SimpleFields::drop_table(executor, true, false)
        .await
        .expect("Failed to drop SimpleNullFields table");
    SimpleFields::create_table(executor, true, true)
        .await
        .expect("Failed to create SimpleNullFields table");

    // Simple 1
    SimpleFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the SimpleNullFields table");
    let entity = SimpleFields {
        alpha: 1,
        bravo: 777.into(),
        charlie: (-2).into(),
        delta: 9876543210.into(),
        echo: None,
        #[cfg(not(feature = "disable-large-integers"))]
        foxtrot: i128::MAX.into(),
        golf: Time::from_hms(12, 0, 10).unwrap().into(),
        hotel: Some("Hello world!".into()),
        india: Box::new(None),
        juliet: true.into(),
        kilo: None,
        lima: Arc::new(Some(3.14)),
        mike: None,
        november: None,
        oscar: None,
        papa: Some(Decimal::from_f32(45.2).unwrap().into()),
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save simple 1");
    let entity = SimpleFields::find_one(executor, &true)
        .await
        .expect("Failed to query simple 1")
        .expect("Failed to find simple 1");
    assert_eq!(entity.alpha, 1);
    assert_eq!(entity.bravo, Some(777));
    assert_eq!(entity.charlie, Some(-2));
    assert_eq!(entity.delta, Some(9876543210));
    assert_eq!(entity.echo, None);
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(
        entity.foxtrot,
        Some(170_141_183_460_469_231_731_687_303_715_884_105_727)
    );
    assert_eq!(entity.golf, Some(Time::from_hms(12, 0, 10).unwrap()));
    assert_eq!(entity.hotel, Some("Hello world!".into()));
    assert_eq!(*entity.india, None);
    assert_eq!(entity.juliet, Some(true));
    assert_eq!(entity.kilo, None);
    assert_eq!(*entity.lima, Some(3.14));
    assert_eq!(entity.mike, None);
    assert_eq!(entity.november, None);
    assert_eq!(entity.oscar, None);
    assert_eq!(entity.papa, Some(Decimal::from_f32(45.2).unwrap().into()));

    // Simple 1 - multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let writer = executor.driver().sql_writer();
        let mut query = RawQuery::default();
        writer.write_delete::<SimpleFields>(&mut query, &false); // Does not delete anything
        writer.write_select(
            &mut query,
            SimpleFields::columns(),
            SimpleFields::table(),
            &true,
            None,
        );
        {
            let mut stream = pin!(executor.run(query));
            let result = stream
                .try_next()
                .await
                .expect("Could not process the first query correctly");
            let Some(QueryResult::Affected(RowsAffected { rows_affected, .. })) = result else {
                panic!("Unexpected result type:\n{result:#?}");
            };
            if let Some(rows_affected) = rows_affected {
                assert_eq!(rows_affected, 0);
            }
            let result = stream
                .try_next()
                .await
                .expect("Could not process the second query correctly");
            let Some(QueryResult::Row(row)) = result else {
                panic!("Unexpected result type:\n{result:#?}");
            };
            let value =
                SimpleFields::from_row(row).expect("Could not decode the row into SimpleFields");
            assert_eq!(
                value,
                SimpleFields {
                    alpha: 1,
                    bravo: 777.into(),
                    charlie: (-2).into(),
                    delta: 9876543210.into(),
                    echo: None,
                    #[cfg(not(feature = "disable-large-integers"))]
                    foxtrot: i128::MAX.into(),
                    golf: Time::from_hms(12, 0, 10).unwrap().into(),
                    hotel: Some("Hello world!".into()),
                    india: Box::new(None),
                    juliet: true.into(),
                    kilo: None,
                    lima: Arc::new(Some(3.14)),
                    mike: None,
                    november: None,
                    oscar: None,
                    papa: Some(Decimal::from_f32(45.2).unwrap().into()),
                }
            );
        }
    }

    // Simple 2
    SimpleFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the SimpleNullFields table");
    let entity = SimpleFields {
        alpha: 255,
        bravo: None,
        charlie: None,
        delta: None,
        echo: Some(Uuid::parse_str("5e915574-bb30-4430-98cf-c5854f61fbbd").unwrap()),
        #[cfg(not(feature = "disable-large-integers"))]
        foxtrot: None,
        golf: None,
        hotel: None,
        india: Box::new(None),
        juliet: None,
        kilo: 4294967295.into(),
        lima: Arc::new(None),
        mike: date!(2025 - 09 - 07).into(),
        november: Cell::new(Decimal::from_f32(1.5).unwrap().into()).into(),
        oscar: RefCell::new(Decimal::from_f32(5080.6244).unwrap().into()).into(),
        papa: None,
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save simple 2");
    let entity = SimpleFields::find_one(executor, &true)
        .await
        .expect("Failed to query simple 2")
        .expect("Failed to find simple 2");
    assert_eq!(entity.alpha, 255);
    assert_eq!(entity.bravo, None);
    assert_eq!(entity.charlie, None);
    assert_eq!(entity.delta, None);
    assert_eq!(
        entity.echo,
        Some(Uuid::parse_str("5e915574-bb30-4430-98cf-c5854f61fbbd").unwrap())
    );
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(entity.foxtrot, None);
    assert_eq!(entity.golf, None);
    assert_eq!(entity.hotel, None);
    assert_eq!(*entity.india, None);
    assert_eq!(entity.juliet, None);
    assert_eq!(entity.kilo, Some(4294967295));
    assert_eq!(*entity.lima, None);
    assert_eq!(entity.mike, Some(date!(2025 - 09 - 07)));
    assert_eq!(
        entity.november,
        Some(Cell::new(Decimal::from_f32(1.5).unwrap().into()))
    );
    assert_eq!(
        entity.oscar,
        Some(RefCell::new(Decimal::from_f32(5080.6244).unwrap().into()))
    );
    assert_eq!(entity.papa, None);

    // Simple 2 - multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let writer = executor.driver().sql_writer();
        let mut query = RawQuery::default();
        writer.write_delete::<SimpleFields>(&mut query, &true);
        writer.write_insert(&mut query, [&entity], false);
        writer.write_select(
            &mut query,
            SimpleFields::columns(),
            SimpleFields::table(),
            &true,
            None,
        );
        {
            let mut stream = pin!(executor.run(query));
            let Ok(Some(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
                stream.try_next().await
            else {
                panic!("Could not process the first query correctly")
            };
            if let Some(rows_affected) = rows_affected {
                assert_eq!(rows_affected, 1);
            }
            let Ok(Some(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
                stream.try_next().await
            else {
                panic!("Could not process the first query correctly")
            };
            if let Some(rows_affected) = rows_affected {
                assert_eq!(rows_affected, 1);
            }
            let Ok(Some(QueryResult::Row(row))) = stream.try_next().await else {
                panic!("Could not process the second query correctly")
            };
            let value =
                SimpleFields::from_row(row).expect("Could not decode the row into SimpleFields");
            assert_eq!(
                value,
                SimpleFields {
                    alpha: 255,
                    bravo: None,
                    charlie: None,
                    delta: None,
                    echo: Some(Uuid::parse_str("5e915574-bb30-4430-98cf-c5854f61fbbd").unwrap()),
                    #[cfg(not(feature = "disable-large-integers"))]
                    foxtrot: None,
                    golf: None,
                    hotel: None,
                    india: Box::new(None),
                    juliet: None,
                    kilo: 4294967295.into(),
                    lima: Arc::new(None),
                    mike: date!(2025 - 09 - 07).into(),
                    november: Cell::new(Decimal::from_f32(1.5).unwrap().into()).into(),
                    oscar: RefCell::new(Decimal::from_f32(5080.6244).unwrap().into()).into(),
                    papa: None,
                }
            );
        }
    }
}
