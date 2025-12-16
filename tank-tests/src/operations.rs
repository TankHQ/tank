use std::{pin::pin, sync::LazyLock};
use tank::{
    DataSet, Driver, Entity, Executor, QueryResult, Result, RowsAffected, SqlWriter, cols, expr,
    join,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Month, OffsetDateTime, Time, UtcOffset, macros::date};
use tokio::sync::Mutex;
use uuid::Uuid;

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
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn operations<E: Executor>(executor: &mut E) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    RadioLog::drop_table(executor, true, false).await?;
    Operator::drop_table(executor, true, false).await?;

    Operator::create_table(executor, false, true).await?;
    RadioLog::create_table(executor, false, false).await?;

    // Insert
    let operator = Operator {
        id: Uuid::new_v4(),
        callsign: "SteelHammer".into(),
        service_rank: "Major".into(),
        enlisted: date!(2015 - 06 - 20),
        is_certified: true,
    };
    Operator::insert_one(executor, &operator).await?;

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

    // Find
    let found = Operator::find_pk(executor, &operator.primary_key()).await?;
    if let Some(op) = found {
        log::debug!("Found operator: {:?}", op.callsign);
    }

    if let Some(radio_log) =
        RadioLog::find_one(executor, &expr!(RadioLog::unit_callsign == "Alpha-1")).await?
    {
        log::debug!("Found radio log: {:?}", radio_log.id);
    }

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

    // Save
    let mut operator = operator;
    operator.callsign = "SteelHammerX".into();
    operator.save(executor).await?;

    let mut log = RadioLog::find_one(executor, &expr!(RadioLog::message == "Ping #2"))
        .await?
        .expect("Missing log");
    log.message = "Ping #2 ACK".into();
    log.save(executor).await?;

    // Delete
    RadioLog::delete_one(executor, log.primary_key()).await?;

    let operator_id = operator.id;
    RadioLog::delete_many(executor, &expr!(RadioLog::operator == #operator_id)).await?;

    operator.delete(executor).await?;

    // Prepare
    let mut query =
        RadioLog::prepare_find(executor, &expr!(RadioLog::signal_strength > ?), None).await?;
    query.bind(40)?;
    let _messages: Vec<_> = executor
        .fetch(query)
        .map_ok(|row| row.values[0].clone())
        .try_collect()
        .await?;

    // Multi-Statement
    let writer = executor.driver().sql_writer();
    let mut sql = String::new();
    writer.write_delete::<RadioLog>(&mut sql, &expr!(RadioLog::signal_strength < 10));
    writer.write_insert(&mut sql, [&operator], false);
    writer.write_insert(
        &mut sql,
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
        &mut sql,
        RadioLog::columns(),
        RadioLog::table(),
        &expr!(true),
        Some(50),
    );
    {
        let mut stream = pin!(executor.run(sql));
        while let Some(result) = stream.try_next().await? {
            match result {
                QueryResult::Row(row) => log::debug!("Row: {row:?}"),
                QueryResult::Affected(RowsAffected { rows_affected, .. }) => {
                    log::debug!("Affected rows: {rows_affected:?}")
                }
            }
        }
    }

    Ok(())
}

pub async fn advanced_operations<E: Executor>(executor: &mut E) -> Result<()> {
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
    Operator::insert_many(executor, &operators)
        .await
        .expect("Could not insert operators");
    RadioLog::insert_many(executor, &radio_logs)
        .await
        .expect("Could not insert radio logs");

    let messages = join!(Operator JOIN RadioLog ON Operator::id == RadioLog::operator)
        .select(
            executor,
            cols!(
                RadioLog::signal_strength as strength DESC,
                Operator::callsign ASC,
                RadioLog::message,
            ),
            // X != Y as LIKE => X NOT LIKE Y
            &expr!(Operator::is_certified && RadioLog::message != "Radio check%" as LIKE),
            Some(100),
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
    Ok(())
}
