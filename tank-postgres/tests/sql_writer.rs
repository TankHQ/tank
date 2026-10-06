#[cfg(test)]
mod tests {
    use tank_core::{Context, Driver, DynQuery, Fragment, SqlValueWriter, Value};
    use tank_postgres::PostgresDriver;
    use time::{Date, Month, PrimitiveDateTime, Time, UtcOffset};

    #[test]
    fn sub_minute_offset_is_preserved() {
        let writer = PostgresDriver::default().sql_writer();
        let offset = UtcOffset::from_whole_seconds(19 * 60 + 32).unwrap();
        let value = PrimitiveDateTime::new(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(12, 0, 0).unwrap(),
        )
        .assume_offset(offset);
        let mut out = DynQuery::default();
        writer.write_value(
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
            &Value::TimestampWithTimezone(Some(value)),
        );
        let sql = out.as_str().into_owned();
        assert!(
            sql.contains("+00:19:32"),
            "Sub-minute offset seconds were dropped, shifting the instant: {sql}"
        );
    }
}
