#![allow(unused_imports)]
use std::{pin::pin, sync::LazyLock};
use tank::{
    Driver, DynQuery, Entity, Executor, QueryBuilder, QueryResult, Result, RowsAffected, SqlWriter,
    anyhow, cols, expr, join,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, OffsetDateTime, Time, UtcOffset, macros::date};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// #region operations-schema
#[derive(Entity)]
#[tank(schema = "operations", name = "radio_operator")]
pub struct Operator {
    #[tank(primary_key)]
    pub id: Uuid,
    pub callsign: String,
    #[tank(name = "rank")]
    pub service_rank: String,
    #[tank(name = "enlistment_date")]
    pub enlisted: Date,
    pub is_certified: bool,
}

#[derive(Entity)]
#[tank(schema = "operations")]
pub struct RadioLog {
    #[tank(primary_key)]
    pub id: Uuid,
    #[tank(references = Operator::id)]
    pub operator: Uuid,
    pub message: String,
    pub unit_callsign: String,
    #[tank(name = "tx_time")]
    pub transmission_time: OffsetDateTime,
    #[tank(name = "rssi")]
    pub signal_strength: i8,
}
// #endregion operations-schema

pub async fn operations(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    RadioLog::drop_table(executor, true, false).await?;
    Operator::drop_table(executor, true, false).await?;

    Operator::create_table(executor, false, true).await?;
    RadioLog::create_table(executor, false, false).await?;

    // Insert
    let operator = Operator {
        id: Uuid::parse_str("21c90df5-00db-4062-9f5a-bcfa2e759e78").unwrap(),
        callsign: "SteelHammer".into(),
        service_rank: "Major".into(),
        enlisted: date!(2015 - 06 - 20),
        is_certified: true,
    };
    Operator::insert_one(executor, &operator).await?;

    // #region operations-insert-many
    let op_id = operator.id;
    let logs: Vec<RadioLog> = (0..5)
        .map(|i| RadioLog {
            id: Uuid::new_v4(),
            operator: op_id,
            message: format!("Ping #{i}"),
            unit_callsign: "Alpha-1".into(),
            transmission_time: OffsetDateTime::now_utc(),
            signal_strength: 42,
        })
        .collect();
    RadioLog::insert_many(executor, &logs).await?;
    // #endregion operations-insert-many

    // Find
    if let Some(radio_log) =
        RadioLog::find_one(executor, expr!(RadioLog::unit_callsign == "Alpha-1")).await?
    {
        log::debug!("Found radio log: {:?}", radio_log.id);
    }

    // #region operations-stream
    {
        let mut stream = pin!(RadioLog::find_many(
            executor,
            expr!(RadioLog::signal_strength >= 40),
            Some(100)
        ));
        while let Some(radio_log) = stream.try_next().await? {
            log::debug!("Found radio log: {:?}", radio_log.id);
        }
        // Executor is released from the stream at the end of the scope
    }
    // #endregion operations-stream

    // Save
    let mut operator = operator;
    operator.callsign = "SteelHammerX".into();
    operator.save(executor).await?;

    let mut log = RadioLog::find_one(executor, expr!(RadioLog::message == "Ping #2"))
        .await?
        .ok_or_else(|| anyhow!("Missing log"))?;
    log.message = "Ping #2 ACK".into();
    log.save(executor).await?;

    // Prepare
    // #region operations-prepared
    let mut query =
        RadioLog::prepare_find(executor, expr!(RadioLog::signal_strength > ?), None).await?;
    query.bind(40)?;
    let messages: Vec<_> = executor
        .fetch(query)
        .map(|row| row.and_then(RadioLog::from_row).map(|log| log.message))
        .try_collect()
        .await?;
    // #endregion operations-prepared
    assert_eq!(messages.len(), 5);
    assert!(
        ["Ping #0", "Ping #1", "Ping #2 ACK", "Ping #3", "Ping #4"]
            .into_iter()
            .all(|message| messages.iter().any(|actual| actual.as_str() == message))
    );

    // Delete
    RadioLog::delete_many(executor, log.primary_key_expr()).await?;

    let operator_id = operator.id;
    RadioLog::delete_many(executor, expr!(RadioLog::operator == #operator_id)).await?;

    operator.delete(executor).await?;

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        // #region operations-batch
        let writer = executor.driver().sql_writer();
        let mut query = DynQuery::default();
        writer.write_delete::<RadioLog>(&mut query, expr!(RadioLog::signal_strength < 10));
        writer.write_insert(&mut query, [&operator], false);
        writer.write_insert(
            &mut query,
            [&RadioLog {
                id: Uuid::new_v4(),
                operator: operator.id,
                message: "Status report".into(),
                unit_callsign: "Alpha-1".into(),
                transmission_time: OffsetDateTime::now_utc(),
                signal_strength: 55,
            }],
            false,
        );
        writer.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(RadioLog::columns())
                .from(RadioLog::table())
                .where_expr(true)
                .limit(Some(50)),
        );
        {
            let mut stream = pin!(executor.run(query));
            while let Some(result) = stream.try_next().await? {
                match result {
                    QueryResult::Row(row) => log::debug!("Row: {row:?}"),
                    QueryResult::Affected(RowsAffected { rows_affected, .. }) => {
                        log::debug!("Affected rows: {rows_affected:?}")
                    }
                }
            }
        }
        // #endregion operations-batch
    }

    Ok(())
}

pub async fn advanced_operations(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    RadioLog::drop_table(executor, true, false).await?;
    Operator::drop_table(executor, true, false).await?;

    Operator::create_table(executor, false, true).await?;
    RadioLog::create_table(executor, false, false).await?;

    let operators = vec![
        Operator {
            id: Uuid::new_v4(),
            callsign: "SteelHammer".into(),
            service_rank: "Major".into(),
            enlisted: date!(2015 - 06 - 20),
            is_certified: true,
        },
        Operator {
            id: Uuid::new_v4(),
            callsign: "Viper".into(),
            service_rank: "Sgt".into(),
            enlisted: date!(2019 - 11 - 01),
            is_certified: true,
        },
        Operator {
            id: Uuid::new_v4(),
            callsign: "Rook".into(),
            service_rank: "Pvt".into(),
            enlisted: date!(2023 - 01 - 15),
            is_certified: false,
        },
    ];
    let radio_logs = vec![
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[0].id,
            message: "Radio check, channel 3. How copy?".into(),
            unit_callsign: "Alpha-1".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 4).unwrap(),
                Time::from_hms(19, 45, 21).unwrap(),
                UtcOffset::from_hms(1, 0, 0).unwrap(),
            ),
            signal_strength: -42,
        },
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[0].id,
            message: "Target acquired. Requesting coordinates.".into(),
            unit_callsign: "Alpha-1".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 4).unwrap(),
                Time::from_hms(19, 54, 12).unwrap(),
                UtcOffset::from_hms(1, 0, 0).unwrap(),
            ),
            signal_strength: -55,
        },
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[0].id,
            message: "Heavy armor spotted, grid 4C.".into(),
            unit_callsign: "Alpha-1".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 4).unwrap(),
                Time::from_hms(19, 51, 9).unwrap(),
                UtcOffset::from_hms(1, 0, 0).unwrap(),
            ),
            signal_strength: -52,
        },
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[1].id,
            message: "Perimeter secure. All clear.".into(),
            unit_callsign: "Bravo-2".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 4).unwrap(),
                Time::from_hms(19, 51, 9).unwrap(),
                UtcOffset::from_hms(1, 0, 0).unwrap(),
            ),
            signal_strength: -68,
        },
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[2].id,
            message: "Radio check, grid 1A. Over.".into(),
            unit_callsign: "Charlie-3".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 4).unwrap(),
                Time::from_hms(18, 59, 11).unwrap(),
                UtcOffset::from_hms(2, 0, 0).unwrap(),
            ),
            signal_strength: -41,
        },
        RadioLog {
            id: Uuid::new_v4(),
            operator: operators[0].id,
            message: "Affirmative, engaging.".into(),
            unit_callsign: "Alpha-1".into(),
            transmission_time: OffsetDateTime::new_in_offset(
                Date::from_calendar_date(2025, Month::November, 3).unwrap(),
                Time::from_hms(23, 11, 54).unwrap(),
                UtcOffset::from_hms(0, 0, 0).unwrap(),
            ),
            signal_strength: -54,
        },
    ];
    Operator::insert_many(executor, &operators).await?;
    RadioLog::insert_many(executor, &radio_logs).await?;

    #[cfg(not(feature = "disable-joins"))]
    {
        // #region operations-select-order
        let messages = executor
            .fetch(
                QueryBuilder::new()
                    .select(cols!(
                        RadioLog::signal_strength as strength,
                        Operator::callsign,
                        RadioLog::message,
                    ))
                    .from(join!(Operator JOIN RadioLog ON Operator::id == RadioLog::operator))
                    .where_expr(expr!(
                        // X != Y as LIKE => X NOT LIKE Y
                        Operator::is_certified && RadioLog::message != "Radio check%" as LIKE
                    ))
                    .order_by(cols!(RadioLog::signal_strength DESC, Operator::callsign ASC))
                    .limit(Some(100))
                    .build(&executor.driver()),
            )
            .map(|row| {
                row.and_then(|row| {
                    #[derive(Entity)]
                    struct Row {
                        message: String,
                        callsign: String,
                    }
                    Row::from_row(row).and_then(|row| Ok((row.message, row.callsign)))
                })
            })
            .try_collect::<Vec<_>>()
            .await?;
        assert!(
            messages.iter().map(|(a, b)| (a.as_str(), b.as_str())).eq([
                ("Heavy armor spotted, grid 4C.", "SteelHammer"),
                ("Affirmative, engaging.", "SteelHammer"),
                ("Target acquired. Requesting coordinates.", "SteelHammer"),
                ("Perimeter secure. All clear.", "Viper"),
            ]
            .into_iter())
        );
        // #endregion operations-select-order
    }
    Ok(())
}
