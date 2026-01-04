use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::sync::LazyLock;
use tank::{AsValue, DataSet, Entity, Executor, cols, expr, stream::TryStreamExt};
use time::{Date, Month, PrimitiveDateTime, Time};
use tokio::sync::Mutex;

#[derive(Entity, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Times {
    pub timestamp_1: PrimitiveDateTime,
    pub timestamp_2: chrono::NaiveDateTime,
    pub date_1: Date,
    pub date_2: NaiveDate,
    pub time_1: time::Time,
    pub time_2: NaiveTime,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn times<E: Executor>(executor: &mut E) {
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
    let mut query_timestamp_1 = Times::table()
        .prepare(
            executor,
            cols!(Times::timestamp_1 DESC),
            &expr!(Times::timestamp_2 > ?),
            None,
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
    let mut query_timestamp_2 = Times::table()
        .prepare(
            executor,
            cols!(Times::timestamp_2 ASC),
            &expr!(Times::timestamp_1 <= ?),
            None,
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
    let mut query_time_1 = Times::table()
        .prepare(executor, cols!(Times::time_1 DESC), &true, None)
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
}
