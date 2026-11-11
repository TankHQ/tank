#![allow(unused_imports)]
use anyhow::Context;
use rust_decimal::Decimal;
use std::{slice, sync::LazyLock};
use tank::{
    AsValue, Entity, Executor, FixedDecimal, Result, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, Time};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq, Clone)]
#[tank(schema = "testing", name = "bound_types")]
pub struct BoundTypes {
    #[tank(primary_key)]
    pub id: u8,
    pub boolean: bool,
    pub int8: i8,
    pub uint8: u8,
    pub int16: i16,
    pub uint16: u16,
    pub int32: i32,
    pub uint32: u32,
    pub int64: i64,
    pub float32: f32,
    pub float64: f64,
    pub text: String,
    pub uuid: Uuid,
    pub date: Date,
    pub time: Time,
    pub decimal: FixedDecimal<20, 4>,
}

fn sample() -> BoundTypes {
    BoundTypes {
        id: 7,
        boolean: true,
        int8: -8,
        uint8: 8,
        int16: -16,
        uint16: 16,
        int32: -32,
        uint32: 32,
        int64: -64,
        float32: 1.5,
        float64: 2.5,
        text: "bound text".into(),
        uuid: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        date: Date::from_calendar_date(2025, Month::June, 15).unwrap(),
        time: Time::from_hms(10, 30, 0).unwrap(),
        decimal: Decimal::new(12356, 4).into(),
    }
}

fn sample_other() -> BoundTypes {
    BoundTypes {
        id: 8,
        boolean: false,
        int8: 127,
        uint8: 255,
        int16: 32_767,
        uint16: 65_535,
        int32: 2_147_483_647,
        uint32: 4_294_967_295,
        int64: 9_223_372_036_854_775_807,
        float32: -3.25,
        float64: -6.5,
        text: "other".into(),
        uuid: Uuid::nil(),
        date: Date::from_calendar_date(2000, Month::January, 1).unwrap(),
        time: Time::from_hms(23, 59, 59).unwrap(),
        decimal: Decimal::new(-1, 0).into(),
    }
}

pub async fn bindings(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    crate::silent_logs! {
        BoundTypes::drop_table(executor, true, false).await?;
    }
    BoundTypes::create_table(executor, true, true).await?;

    let first = sample();
    let second = sample_other();
    BoundTypes::insert_many(executor, [first.clone(), second.clone()]).await?;

    let mut query = BoundTypes::prepare_find(
        executor,
        expr!(
            BoundTypes::boolean == ?
                && BoundTypes::int8 == ?
                && BoundTypes::uint8 == ?
                && BoundTypes::int16 == ?
                && BoundTypes::uint16 == ?
                && BoundTypes::int32 == ?
                && BoundTypes::uint32 == ?
                && BoundTypes::int64 == ?
                && BoundTypes::float32 == ?
                && BoundTypes::float64 == ?
                && BoundTypes::text == ?
                && BoundTypes::uuid == ?
                && BoundTypes::date == ?
                && BoundTypes::time == ?
                && BoundTypes::decimal == ?
        ),
        Some(1),
    )
    .await?;
    for (name, value) in [
        ("first.boolean", first.boolean.as_value()),
        ("first.int8", first.int8.as_value()),
        ("first.uint8", first.uint8.as_value()),
        ("first.int16", first.int16.as_value()),
        ("first.uint16", first.uint16.as_value()),
        ("first.int32", first.int32.as_value()),
        ("first.uint32", first.uint32.as_value()),
        ("first.int64", first.int64.as_value()),
        ("first.float32", first.float32.as_value()),
        ("first.float64", first.float64.as_value()),
        ("first.text", first.text.clone().as_value()),
        ("first.uuid", first.uuid.as_value()),
        ("first.date", first.date.as_value()),
        ("first.time", first.time.as_value()),
        ("first.decimal", first.decimal.as_value()),
    ] {
        query
            .bind(value.clone())
            .context(format!("While binding to bind column {name}: {value:?}"))?;
    }
    let loaded = executor
        .fetch(&mut query)
        .map(|row| row.and_then(BoundTypes::from_row))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded.as_slice(),
        slice::from_ref(&first),
        "Bound values did not round-trip"
    );

    query.clear_bindings()?;
    for (name, value) in [
        ("second.boolean", second.boolean.as_value()),
        ("second.int8", second.int8.as_value()),
        ("second.uint8", second.uint8.as_value()),
        ("second.int16", second.int16.as_value()),
        ("second.uint16", second.uint16.as_value()),
        ("second.int32", second.int32.as_value()),
        ("second.uint32", second.uint32.as_value()),
        ("second.int64", second.int64.as_value()),
        ("second.float32", second.float32.as_value()),
        ("second.float64", second.float64.as_value()),
        ("second.text", second.text.clone().as_value()),
        ("second.uuid", second.uuid.as_value()),
        ("second.date", second.date.as_value()),
        ("second.time", second.time.as_value()),
        ("second.decimal", second.decimal.as_value()),
    ] {
        query
            .bind(value.clone())
            .context(format!("While binding to bind column {name}: {value:?}"))?;
    }
    let loaded = executor
        .fetch(&mut query)
        .map(|row| row.and_then(BoundTypes::from_row))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded.as_slice(),
        slice::from_ref(&second),
        "Reused statement did not re-read the bindings"
    );

    Ok(())
}
