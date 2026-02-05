#![allow(dead_code)]
#![allow(unused_imports)]
use crate::silent_logs;
use std::{pin::pin, sync::LazyLock, time::Duration};
use tank::{
    Driver, DynQuery, Entity, Executor, Interval, QueryBuilder, QueryResult, RawQuery,
    RowsAffected, SqlWriter,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity)]
struct Intervals {
    #[tank(primary_key)]
    pk: u32,
    first: time::Duration,
    second: Interval,
    third: Duration,
}

pub async fn interval<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Intervals::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Intervals table");
    Intervals::create_table(executor, true, true)
        .await
        .expect("Failed to create Intervals table");

    Intervals::insert_one(
        executor,
        &Intervals {
            pk: 1,
            first: Default::default(),
            second: Default::default(),
            third: Default::default(),
        },
    )
    .await
    .expect("Insert zero intervals failed");
    let value = Intervals::find_one(executor, true)
        .await
        .expect("Failed to retrieve zero intervals")
        .expect("Missing zero interval row");
    assert_eq!(value.pk, 1);
    assert_eq!(value.first, time::Duration::default());
    assert_eq!(value.second, Interval::default());
    assert_eq!(value.third, Duration::default());
    Intervals::delete_many(executor, true)
        .await
        .expect("Could not delete the intervals");

    Intervals::insert_one(
        executor,
        &Intervals {
            pk: 2,
            first: time::Duration::minutes(1) + time::Duration::days(1),
            #[cfg(not(feature = "disable-large-intervals"))]
            second: Interval::from_years(1_000),
            #[cfg(feature = "disable-large-intervals")]
            second: Interval::from_mins(5) + Interval::from_secs(24) + Interval::from_millis(33),
            third: Duration::from_micros(1) + Duration::from_hours(6),
        },
    )
    .await
    .expect("Could not insert the interval");
    let value = Intervals::find_one(executor, true)
        .await
        .expect("Could not retrieve the intervals row")
        .expect("There was no interval inserted in the table intervals");
    assert_eq!(value.pk, 2);
    assert_eq!(value.first, time::Duration::minutes(1 + 24 * 60));
    #[cfg(not(feature = "disable-large-intervals"))]
    assert_eq!(value.second, Interval::from_months(1_000 * 12));
    #[cfg(feature = "disable-large-intervals")]
    assert_eq!(
        value.second,
        Interval::from_mins(5) + Interval::from_secs(24) + Interval::from_millis(33)
    );
    assert_eq!(value.third, Duration::from_micros(1 + 6 * 3600 * 1_000_000));
    Intervals::delete_many(executor, true)
        .await
        .expect("Could not delete the intervals");

    Intervals::insert_one(
        executor,
        &Intervals {
            pk: 3,
            #[cfg(not(feature = "disable-large-intervals"))]
            first: time::Duration::weeks(52) + time::Duration::hours(3),
            #[cfg(feature = "disable-large-intervals")]
            first: time::Duration::weeks(1) + time::Duration::seconds(1),
            second: Interval::from_days(-11),
            third: Duration::from_micros(999_999_999),
        },
    )
    .await
    .expect("Insert large intervals failed");
    let mut value = Intervals::find_one(executor, true)
        .await
        .expect("Failed to retrieve large intervals")
        .expect("Missing large interval row");
    assert_eq!(value.pk, 3);
    #[cfg(not(feature = "disable-large-intervals"))]
    assert_eq!(
        value.first,
        time::Duration::weeks(52) + time::Duration::hours(3)
    );
    #[cfg(feature = "disable-large-intervals")]
    assert_eq!(
        value.first,
        time::Duration::weeks(1) + time::Duration::seconds(1),
    );
    assert_eq!(value.second, -Interval::from_days(11));
    assert_eq!(value.third, Duration::from_micros(999_999_999));
    value.third += Duration::from_micros(1);

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = DynQuery::default();
        let writer = executor.driver().sql_writer();
        writer.write_delete::<Intervals>(&mut query, true);
        writer.write_insert(
            &mut query,
            &[
                Intervals {
                    pk: 4,
                    first: time::Duration::weeks(4) + time::Duration::hours(5),
                    #[cfg(not(feature = "disable-large-intervals"))]
                    second: Interval::from_years(20_000) + Interval::from_millis(300),
                    #[cfg(feature = "disable-large-intervals")]
                    second: Interval::from_hours(3) + Interval::from_millis(300),
                    third: Duration::from_secs(0),
                },
                Intervals {
                    pk: 5,
                    first: time::Duration::minutes(20) + time::Duration::milliseconds(1),
                    second: Interval::from_months(4) + Interval::from_days(2),
                    third: Duration::from_nanos(5000),
                },
            ],
            false,
        );
        writer.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(Intervals::columns())
                .from(Intervals::table())
                .where_expr(true),
        );
        let mut stream = pin!(executor.run(query));

        // DELETE
        let Some(Ok(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
            stream.next().await
        else {
            panic!("Could not get the result of deleting the rows")
        };
        if let Some(rows_affected) = rows_affected {
            assert_eq!(rows_affected, 1);
        }

        // INSERT
        let Some(Ok(QueryResult::Affected(RowsAffected { rows_affected, .. }))) =
            stream.next().await
        else {
            panic!("Could not get the result of inserting the rows")
        };
        if let Some(rows_affected) = rows_affected {
            assert_eq!(rows_affected, 2);
        }

        // SELECT
        let mut intervals = Vec::new();
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the result of selecting the rows")
        };
        intervals.push(Intervals::from_row(row).expect("Could not decode the first Intervals row"));
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the result of selecting the rows")
        };
        intervals
            .push(Intervals::from_row(row).expect("Could not decode the second Intervals row"));
        intervals.sort_by(|a, b| a.pk.cmp(&b.pk));

        // Row 1
        let interval = &intervals[0];
        assert_eq!(interval.pk, 4);
        assert_eq!(interval.first, time::Duration::hours(4 * 7 * 24 + 5));
        #[cfg(not(feature = "disable-large-intervals"))]
        assert_eq!(
            interval.second,
            Interval::from_months(20000 * 12) + Interval::from_micros(300_000)
        );
        #[cfg(feature = "disable-large-intervals")]
        assert_eq!(
            interval.second,
            Interval::from_hours(3) + Interval::from_millis(300),
        );
        assert_eq!(interval.third, Duration::ZERO);

        // Row 2
        let interval = &intervals[1];
        assert_eq!(interval.pk, 5);
        assert_eq!(interval.first, time::Duration::milliseconds(1200001));
        assert_eq!(
            interval.second,
            Interval::from_months(4) + Interval::from_hours(48)
        );
        assert_eq!(interval.third, Duration::from_nanos(5000));
    }
}
