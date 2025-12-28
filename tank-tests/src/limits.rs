#![allow(unused_imports)]
use core::f64;
use std::{pin::pin, sync::LazyLock};
use tank::{Driver, QueryResult, RowsAffected, SqlWriter, expr, stream::StreamExt};
use tank::{Entity, Executor, Interval};
use time::{Date, Month, Time};
use tokio::sync::Mutex;

// Safe max value
const F32_MAX: f32 = 3.4e+38_f32;

#[derive(Entity)]
struct Limits {
    #[tank(primary_key)]
    id: usize,
    boolean: bool,
    int8: i8,
    uint8: u8,
    int16: i16,
    uint16: u16,
    int32: i32,
    uint32: u32,
    int64: i64,
    #[cfg(not(feature = "disable-large-integers"))]
    uint64: u64,
    #[cfg(not(feature = "disable-large-integers"))]
    int128: i128,
    #[cfg(not(feature = "disable-large-integers"))]
    uint128: u128,
    float32: f32,
    float64: f64,
    time: Time,
    date: Date,
    #[cfg(not(feature = "disable-intervals"))]
    interval: Interval,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn limits<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Limits::drop_table(executor, true, false)
        .await
        .expect("Failed to drop SimpleNullFields table");
    Limits::create_table(executor, true, true)
        .await
        .expect("Failed to create SimpleNullFields table");

    // Minimals
    Limits::delete_many(executor, &true)
        .await
        .expect("Failed to clear the Limits table");
    let minimals = Limits {
        id: 1,
        boolean: false,
        int8: -127,
        uint8: 0,
        int16: -32_768,
        uint16: 0,
        int32: -2_147_483_648,
        uint32: 0,
        int64: -9_223_372_036_854_775_808,
        #[cfg(not(feature = "disable-large-integers"))]
        uint64: 0,
        #[cfg(not(feature = "disable-large-integers"))]
        int128: -170_141_183_460_469_231_731_687_303_715_884_105_728,
        #[cfg(not(feature = "disable-large-integers"))]
        uint128: 0,
        float32: f32::MIN_POSITIVE,
        #[cfg(not(feature = "disable-infinity"))]
        float64: f64::NEG_INFINITY,
        #[cfg(feature = "disable-infinity")]
        float64: f64::MIN,
        time: Time::from_hms(0, 0, 0).expect("All zero must be correct time"),
        #[cfg(not(feature = "disable-old-dates"))]
        date: Date::from_calendar_date(-2000, Month::January, 01)
            .expect("Very old date must be correct"),
        #[cfg(feature = "disable-old-dates")]
        date: Date::from_calendar_date(1970, Month::January, 01)
            .expect("Very old date must be correct"),
        #[cfg(not(feature = "disable-intervals"))]
        interval: Interval::from_micros(1),
    };
    Limits::insert_one(executor, &minimals)
        .await
        .expect("Failed to save minimals entity");
    let loaded = Limits::find_one(executor, &true)
        .await
        .expect("Failed to query simple 2")
        .expect("Failed to find simple 2");
    assert_eq!(loaded.id, 1);
    assert_eq!(loaded.boolean, false);
    assert_eq!(loaded.int8, -127);
    assert_eq!(loaded.uint8, 0);
    assert_eq!(loaded.int16, -32_768);
    assert_eq!(loaded.uint16, 0);
    assert_eq!(loaded.int32, -2_147_483_648);
    assert_eq!(loaded.uint32, 0);
    assert_eq!(loaded.int64, -9_223_372_036_854_775_808);
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(loaded.uint64, 0);
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(
        loaded.int128,
        -170_141_183_460_469_231_731_687_303_715_884_105_728
    );
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(loaded.uint128, 0);
    assert!((loaded.float32 - f32::MIN_POSITIVE).abs() < 0.0001);
    #[cfg(not(feature = "disable-infinity"))]
    assert_eq!(loaded.float64, f64::NEG_INFINITY);
    #[cfg(feature = "disable-infinity")]
    assert_eq!(loaded.float64, f64::MIN);
    assert_eq!(loaded.time, Time::from_hms(0, 0, 0).unwrap());
    #[cfg(not(feature = "disable-old-dates"))]
    assert_eq!(
        loaded.date,
        Date::from_calendar_date(-2000, Month::January, 01).unwrap()
    );
    #[cfg(feature = "disable-old-dates")]
    assert_eq!(
        loaded.date,
        Date::from_calendar_date(1970, Month::January, 01).unwrap()
    );
    #[cfg(not(feature = "disable-intervals"))]
    assert_eq!(loaded.interval, Interval::from_micros(1));

    // Maximals
    Limits::delete_many(executor, &true)
        .await
        .expect("Failed to clear the Limits table");
    let maximals = Limits {
        id: 2,
        boolean: true,
        int8: 127,
        uint8: 255,
        int16: 32_767,
        uint16: 65_535,
        int32: 2_147_483_647,
        uint32: 4_294_967_295,
        int64: 9_223_372_036_854_775_807,
        #[cfg(not(feature = "disable-large-integers"))]
        uint64: 18_446_744_073_709_551_615,
        #[cfg(not(feature = "disable-large-integers"))]
        int128: 170_141_183_460_469_231_731_687_303_715_884_105_727,
        #[cfg(not(feature = "disable-large-integers"))]
        uint128: 340_282_366_920_938_463_463_374_607_431_768_211_455,
        float32: F32_MAX,
        #[cfg(not(feature = "disable-infinity"))]
        float64: f64::INFINITY,
        #[cfg(feature = "disable-infinity")]
        float64: f64::MAX,
        time: Time::from_hms_micro(23, 59, 59, 999_999)
            .expect("Close to midnight time must be correct"),
        date: Date::from_calendar_date(9999, Month::December, 31)
            .expect("Very old date must be correct"),
        #[cfg(all(
            not(feature = "disable-intervals"),
            not(feature = "disable-large-intervals"),
        ))]
        interval: Interval::from_years(1_000_000),
        #[cfg(all(
            not(feature = "disable-intervals"),
            feature = "disable-large-intervals"
        ))]
        interval: Interval::from_days(31) + Interval::from_mins(5),
    };
    Limits::insert_one(executor, &maximals)
        .await
        .expect("Failed to save maximals entity");
    let loaded = Limits::find_one(executor, &true)
        .await
        .expect("Failed to query simple 2")
        .expect("Failed to find simple 2");
    assert_eq!(loaded.id, 2);
    assert_eq!(loaded.boolean, true);
    assert_eq!(loaded.int8, 127);
    assert_eq!(loaded.uint8, 255);
    assert_eq!(loaded.int16, 32_767);
    assert_eq!(loaded.uint16, 65_535);
    assert_eq!(loaded.int32, 2_147_483_647);
    assert_eq!(loaded.uint32, 4_294_967_295);
    assert_eq!(loaded.int64, 9_223_372_036_854_775_807);
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(loaded.uint64, 18_446_744_073_709_551_615);
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(
        loaded.int128,
        170_141_183_460_469_231_731_687_303_715_884_105_727
    );
    #[cfg(not(feature = "disable-large-integers"))]
    assert_eq!(
        loaded.uint128,
        340_282_366_920_938_463_463_374_607_431_768_211_455
    );
    assert!((loaded.float32 - F32_MAX).abs() < 0.001);
    #[cfg(not(feature = "disable-infinity"))]
    assert_eq!(loaded.float64, f64::INFINITY);
    #[cfg(feature = "disable-infinity")]
    assert_eq!(loaded.float64, f64::MAX);
    assert!(
        (loaded.time - Time::from_hms_micro(23, 59, 59, 999_999).unwrap()).abs()
            < time::Duration::milliseconds(1),
    );
    assert_eq!(
        loaded.date,
        Date::from_calendar_date(9999, Month::December, 31).unwrap()
    );
    #[cfg(all(
        not(feature = "disable-intervals"),
        not(feature = "disable-large-intervals"),
    ))]
    assert_eq!(loaded.interval, Interval::from_years(1_000_000));
    #[cfg(all(
        not(feature = "disable-intervals"),
        feature = "disable-large-intervals"
    ))]
    assert_eq!(
        loaded.interval,
        Interval::from_days(31) + Interval::from_mins(5)
    );

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = String::new();
        let writer = executor.driver().sql_writer();
        writer.write_delete::<Limits>(&mut query, &true);
        writer.write_insert(&mut query, &[minimals, maximals], false);
        writer.write_select(
            &mut query,
            Limits::columns(),
            Limits::table(),
            &expr!(!Limits::boolean),
            None,
        );
        writer.write_select(
            &mut query,
            Limits::columns(),
            Limits::table(),
            &expr!(Limits::boolean),
            None,
        );
        let mut stream = pin!(executor.run(query));
        let Some(Ok(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
            stream.next().await
        else {
            panic!("Could not get the result of the first statement");
        };
        if let Some(rows_affected) = rows_affected {
            assert_eq!(rows_affected, 1, "Should have deleted one row");
        }
        let Some(Ok(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
            stream.next().await
        else {
            panic!("Could not get the result of the second statement");
        };
        if let Some(rows_affected) = rows_affected {
            assert_eq!(rows_affected, 2, "Should have inserted two rows");
        }
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the row of the third statement");
        };
        let loaded = Limits::from_row(row).expect("Could not decode the Limits from minimals row");
        assert_eq!(loaded.boolean, false);
        assert_eq!(loaded.int8, -127);
        assert_eq!(loaded.uint8, 0);
        assert_eq!(loaded.int16, -32_768);
        assert_eq!(loaded.uint16, 0);
        assert_eq!(loaded.int32, -2_147_483_648);
        assert_eq!(loaded.uint32, 0);
        assert_eq!(loaded.int64, -9_223_372_036_854_775_808);
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(loaded.uint64, 0);
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(
            loaded.int128,
            -170_141_183_460_469_231_731_687_303_715_884_105_728
        );
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(loaded.uint128, 0);
        assert!((loaded.float32 - f32::MIN_POSITIVE).abs() < 0.0001);
        #[cfg(not(feature = "disable-infinity"))]
        assert_eq!(loaded.float64, f64::NEG_INFINITY);
        #[cfg(feature = "disable-infinity")]
        assert_eq!(loaded.float64, f64::MIN);
        assert_eq!(loaded.time, Time::from_hms(0, 0, 0).unwrap());
        assert_eq!(
            loaded.date,
            Date::from_calendar_date(-2000, Month::January, 01).unwrap()
        );
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the row of the third statement");
        };
        let loaded = Limits::from_row(row).expect("Could not decode the Limits from maximals row");
        assert_eq!(loaded.boolean, true);
        assert_eq!(loaded.int8, 127);
        assert_eq!(loaded.uint8, 255);
        assert_eq!(loaded.int16, 32_767);
        assert_eq!(loaded.uint16, 65_535);
        assert_eq!(loaded.int32, 2_147_483_647);
        assert_eq!(loaded.uint32, 4_294_967_295);
        assert_eq!(loaded.int64, 9_223_372_036_854_775_807);
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(loaded.uint64, 18_446_744_073_709_551_615);
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(
            loaded.int128,
            170_141_183_460_469_231_731_687_303_715_884_105_727
        );
        #[cfg(not(feature = "disable-large-integers"))]
        assert_eq!(
            loaded.uint128,
            340_282_366_920_938_463_463_374_607_431_768_211_455
        );
        assert!((loaded.float32 - F32_MAX).abs() < 1E35 && loaded.float32 > 1E38);
        #[cfg(not(feature = "disable-infinity"))]
        assert_eq!(loaded.float64, f64::INFINITY);
        #[cfg(feature = "disable-infinity")]
        assert_eq!(loaded.float64, f64::MIN);
        assert_eq!(
            loaded.time,
            Time::from_hms_micro(23, 59, 59, 999_999).unwrap()
        );
        assert_eq!(
            loaded.date,
            Date::from_calendar_date(9999, Month::December, 31).unwrap()
        );
        #[cfg(all(
            not(feature = "disable-intervals"),
            not(feature = "disable-large-intervals"),
        ))]
        assert_eq!(loaded.interval, Interval::from_years(1_000_000));
        #[cfg(all(
            not(feature = "disable-intervals"),
            feature = "disable-large-intervals"
        ))]
        assert_eq!(
            loaded.interval,
            Interval::from_days(31) + Interval::from_mins(5)
        );
    }
}
