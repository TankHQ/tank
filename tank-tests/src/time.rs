use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::{
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};
use tank::{
    AsValue, Entity, Executor, Operand, QueryBuilder, Result, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Times {
    pub timestamp_1: PrimitiveDateTime,
    pub timestamp_2: chrono::NaiveDateTime,
    pub date_1: Date,
    pub date_2: NaiveDate,
    pub time_1: time::Time,
    pub time_2: NaiveTime,
}

pub async fn times(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    // Setup
    Times::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Times table");
    Times::create_table(executor, false, true)
        .await
        .expect("Failed to create Times table");

    // Insert times
    let timestamps = [
        // 1980-01-01 00:00:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(1980, Month::January, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1980, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(1980, Month::January, 1).unwrap(),
            date_2: NaiveDate::from_ymd_opt(1980, 1, 1).unwrap(),
            time_1: Time::from_hms(0, 0, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        },
        // 1987-10-05 14:09:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(1987, Month::October, 5).unwrap(),
                Time::from_hms(14, 9, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1987, 10, 5).unwrap(),
                NaiveTime::from_hms_opt(14, 9, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(1987, Month::October, 5).unwrap(),
            date_2: NaiveDate::from_ymd_opt(1987, 10, 5).unwrap(),
            time_1: Time::from_hms(14, 9, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(14, 9, 0).unwrap(),
        },
        // 1999-12-31 23:59:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(1999, Month::December, 31).unwrap(),
                Time::from_hms(23, 59, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1999, 12, 31).unwrap(),
                NaiveTime::from_hms_opt(23, 59, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(1999, Month::December, 31).unwrap(),
            date_2: NaiveDate::from_ymd_opt(1999, 12, 31).unwrap(),
            time_1: Time::from_hms(23, 59, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(23, 59, 0).unwrap(),
        },
        // 2025-01-01 00:00:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::January, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(2025, Month::January, 1).unwrap(),
            date_2: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            time_1: Time::from_hms(0, 0, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        },
        // 1950-06-15 08:30:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(1950, Month::June, 15).unwrap(),
                Time::from_hms(8, 30, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(1950, 6, 15).unwrap(),
                NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(1950, Month::June, 15).unwrap(),
            date_2: NaiveDate::from_ymd_opt(1950, 6, 15).unwrap(),
            time_1: Time::from_hms(8, 30, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
        },
        // 2038-01-19 03:14:07
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(2038, Month::January, 19).unwrap(),
                Time::from_hms(3, 14, 7).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2038, 1, 19).unwrap(),
                NaiveTime::from_hms_opt(3, 14, 7).unwrap(),
            ),
            date_1: Date::from_calendar_date(2038, Month::January, 19).unwrap(),
            date_2: NaiveDate::from_ymd_opt(2038, 1, 19).unwrap(),
            time_1: Time::from_hms(3, 14, 7).unwrap(),
            time_2: NaiveTime::from_hms_opt(3, 14, 7).unwrap(),
        },
        // 2025-07-19 09:42:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(2025, Month::July, 19).unwrap(),
                Time::from_hms(9, 42, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2025, 7, 19).unwrap(),
                NaiveTime::from_hms_opt(9, 42, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(2025, Month::July, 19).unwrap(),
            date_2: NaiveDate::from_ymd_opt(2025, 7, 19).unwrap(),
            time_1: Time::from_hms(9, 42, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(9, 42, 0).unwrap(),
        },
        // 2050-11-11 18:45:59
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(2050, Month::November, 11).unwrap(),
                Time::from_hms(18, 45, 59).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2050, 11, 11).unwrap(),
                NaiveTime::from_hms_opt(18, 45, 59).unwrap(),
            ),
            date_1: Date::from_calendar_date(2050, Month::November, 11).unwrap(),
            date_2: NaiveDate::from_ymd_opt(2050, 11, 11).unwrap(),
            time_1: Time::from_hms(18, 45, 59).unwrap(),
            time_2: NaiveTime::from_hms_opt(18, 45, 59).unwrap(),
        },
        // 2050-09-09 00:00:00
        Times {
            timestamp_1: PrimitiveDateTime::new(
                Date::from_calendar_date(2050, Month::September, 9).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            ),
            timestamp_2: NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2050, 9, 9).unwrap(),
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            date_1: Date::from_calendar_date(2050, Month::September, 9).unwrap(),
            date_2: NaiveDate::from_ymd_opt(2050, 9, 9).unwrap(),
            time_1: Time::from_hms(0, 0, 0).unwrap(),
            time_2: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        },
    ];

    Times::insert_many(executor, &timestamps)
        .await
        .expect("Failed to insert timestamps");

    /*
     * 1987-10-05 14:09:00
     * 1999-12-31 23:59:00
     * 2025-01-01 00:00:00
     * 1950-06-15 08:30:00
     * 1900-01-01 00:00:00
     * 2038-01-19 03:14:07
     * 2025-07-19 09:42:00
     * 2050-11-11 18:45:59
     * 2050-09-09 00:00:00
     */

    // Query timestamp 1
    let mut query_timestamp_1 = executor
        .prepare(
            QueryBuilder::new()
                .select([Times::timestamp_1])
                .from(Times::table())
                .where_expr(expr!(Times::timestamp_2 > ?))
                .order_by(cols!(Times::timestamp_1 DESC))
                .build(&executor.driver()),
        )
        .await
        .expect("Could not prepare the query timestamp 1");
    query_timestamp_1
        .bind(PrimitiveDateTime::new(
            Date::from_calendar_date(1999, Month::December, 31).unwrap(),
            Time::from_hms(23, 59, 59).unwrap(),
        ))
        .expect("Could not bind the timestamp 1");
    let values = executor
        .fetch(&mut query_timestamp_1)
        .and_then(|v| async {
            PrimitiveDateTime::try_from_value(
                v.values
                    .into_iter()
                    .next()
                    .expect("Could not get the first column"),
            )
            .map(|v| v.to_string())
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not collect the values from the stream");
    assert_eq!(
        values,
        [
            "2050-11-11 18:45:59.0",
            "2050-09-09 0:00:00.0",
            "2038-01-19 3:14:07.0",
            "2025-07-19 9:42:00.0",
            "2025-01-01 0:00:00.0",
        ]
    );
    query_timestamp_1
        .clear_bindings()
        .expect("Could not clear the bindings for query timestamp 1");
    query_timestamp_1
        .bind(PrimitiveDateTime::new(
            Date::from_calendar_date(1800, Month::January, 1).unwrap(),
            Time::from_hms(0, 0, 0).unwrap(),
        ))
        .expect("Could not bind the timestamp 1");
    let values = executor
        .fetch(&mut query_timestamp_1)
        .and_then(|v| async {
            NaiveDateTime::try_from_value(
                v.values
                    .into_iter()
                    .next()
                    .expect("Could not get the first column"),
            )
            .map(|v| v.to_string())
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not collect the values from the stream");
    assert_eq!(
        values,
        [
            "2050-11-11 18:45:59",
            "2050-09-09 00:00:00",
            "2038-01-19 03:14:07",
            "2025-07-19 09:42:00",
            "2025-01-01 00:00:00",
            "1999-12-31 23:59:00",
            "1987-10-05 14:09:00",
            "1980-01-01 00:00:00",
            "1950-06-15 08:30:00",
        ]
    );

    // Query timestamp 2
    let mut query_timestamp_2 = executor
        .prepare(
            QueryBuilder::new()
                .select([Times::timestamp_2])
                .from(Times::table())
                .where_expr(expr!(Times::timestamp_1 <= ?))
                .order_by(cols!(Times::timestamp_2 ASC))
                .build(&executor.driver()),
        )
        .await
        .expect("Could not prepare the query timestamp 1");
    query_timestamp_2
        .bind(NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ))
        .expect("Could not bind the timestamp 1");
    let values = executor
        .fetch(&mut query_timestamp_2)
        .and_then(|v| async {
            NaiveDateTime::try_from_value(
                v.values
                    .into_iter()
                    .next()
                    .expect("Could not get the first column"),
            )
            .map(|v| v.to_string())
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not collect the values from the stream");
    assert_eq!(
        values,
        [
            "1950-06-15 08:30:00",
            "1980-01-01 00:00:00",
            "1987-10-05 14:09:00",
            "1999-12-31 23:59:00",
            "2025-01-01 00:00:00",
        ]
    );

    // Query time 1
    let mut query_time_1 = executor
        .prepare(
            QueryBuilder::new()
                .select([Times::time_1])
                .from(Times::table())
                .where_expr(true)
                .order_by(cols!(Times::time_1 DESC))
                .build(&executor.driver()),
        )
        .await
        .expect("Could not prepare the query timestamp 1");
    let values = executor
        .fetch(&mut query_time_1)
        .and_then(|v| async {
            NaiveTime::try_from_value(
                v.values
                    .into_iter()
                    .next()
                    .expect("Could not get the first column"),
            )
            .map(|v| v.to_string())
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not collect the values from the stream");
    assert_eq!(
        values,
        [
            "23:59:00", "18:45:59", "14:09:00", "09:42:00", "08:30:00", "03:14:07", "00:00:00",
            "00:00:00", "00:00:00",
        ]
    );

    // Current timestamp ms
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - 2;
    let timestamp_ms = executor
        .fetch(
            QueryBuilder::new()
                .select([&Operand::CurrentTimestampMs])
                .from(Times::table())
                .where_expr(true)
                .limit(Some(1))
                .build(&executor.driver()),
        )
        .map_ok(|v| u128::try_from_value(v.values.into_iter().nth(0).expect("There is no column")))
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the current timestamp");
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 2;
    let timestamp_ms = timestamp_ms.into_iter().next().unwrap();
    assert!(before <= timestamp_ms, "{before} <= {timestamp_ms}");
    assert!(timestamp_ms <= after, "{timestamp_ms} <= {after}");

    // Negative UTC offsets
    {
        #[derive(Entity, Debug, PartialEq)]
        #[tank(name = "tz_offsets")]
        struct TzOffsets {
            #[tank(primary_key)]
            id: i32,
            ts: OffsetDateTime,
        }

        TzOffsets::drop_table(executor, true, false)
            .await
            .expect("Failed to drop TzOffsets table");
        TzOffsets::create_table(executor, true, true)
            .await
            .expect("Failed to create TzOffsets table");

        // UTC-05:30
        let neg_offset = TzOffsets {
            id: 1,
            ts: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::June, 15).unwrap(),
                Time::from_hms(14, 30, 0).unwrap(),
                UtcOffset::from_hms(-5, -30, 0).unwrap(),
            ),
        };
        neg_offset
            .save(executor)
            .await
            .expect("Failed to save entity with negative UTC offset");

        let found = TzOffsets::find_one(executor, expr!(TzOffsets::id == 1))
            .await
            .expect("Failed to query")
            .expect("Entity with negative offset not found");
        assert_eq!(
            found.ts.unix_timestamp(),
            neg_offset.ts.unix_timestamp(),
            "Timestamp instant mismatch after round-trip with negative offset"
        );

        // UTC+05:45
        let nepal = TzOffsets {
            id: 2,
            ts: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::June, 15).unwrap(),
                Time::from_hms(20, 15, 0).unwrap(),
                UtcOffset::from_hms(5, 45, 0).unwrap(),
            ),
        };
        nepal
            .save(executor)
            .await
            .expect("Failed to save entity with Nepal offset");

        let found = TzOffsets::find_one(executor, expr!(TzOffsets::id == 2))
            .await
            .expect("Failed to query")
            .expect("Entity with Nepal offset not found");
        assert_eq!(
            found.ts.unix_timestamp(),
            nepal.ts.unix_timestamp(),
            "Timestamp instant mismatch after round-trip with Nepal offset"
        );

        #[cfg(not(feature = "disable-old-dates"))]
        {
            let bc_offset = TzOffsets {
                id: 3,
                ts: OffsetDateTime::new_in_offset(
                    Date::from_calendar_date(0, Month::March, 15).unwrap(),
                    Time::from_hms(10, 30, 0).unwrap(),
                    UtcOffset::from_hms(2, 0, 0).unwrap(),
                ),
            };
            bc_offset
                .save(executor)
                .await
                .expect("Failed to save entity with BC date and offset");

            let found = TzOffsets::find_one(executor, expr!(TzOffsets::id == 3))
                .await
                .expect("Failed to query")
                .expect("Entity with BC date not found");
            assert_eq!(
                found.ts.unix_timestamp(),
                bc_offset.ts.unix_timestamp(),
                "Timestamp instant mismatch after round-trip with BC date"
            );

            let bc_negative = TzOffsets {
                id: 4,
                ts: OffsetDateTime::new_in_offset(
                    Date::from_calendar_date(-500, Month::July, 1).unwrap(),
                    Time::from_hms(8, 0, 0).unwrap(),
                    UtcOffset::from_hms(-3, 0, 0).unwrap(),
                ),
            };
            bc_negative
                .save(executor)
                .await
                .expect("Failed to save entity with BC date and negative offset");

            let found = TzOffsets::find_one(executor, expr!(TzOffsets::id == 4))
                .await
                .expect("Failed to query")
                .expect("Entity with BC date and negative offset not found");
            assert_eq!(
                found.ts.unix_timestamp(),
                bc_negative.ts.unix_timestamp(),
                "Timestamp instant mismatch after round-trip with BC date and negative offset"
            );
        }
    }
}
