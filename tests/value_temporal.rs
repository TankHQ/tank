#[cfg(test)]
mod tests {
    use rust_decimal::{
        Decimal,
        prelude::{FromPrimitive, Zero},
    };
    use tank_core::{AsValue, Interval, Value};
    use time::Month;
    use uuid::Uuid;

    #[test]
    fn value_date() {
        let var = time::Date::from_calendar_date(2025, Month::July, 21).unwrap();
        let val: Value = var.as_value();
        assert_eq!(val, Value::Date(Some(var)));
        assert_ne!(val, Value::Null);
        let var: time::Date = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Date = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::Date::from_calendar_date(2025, Month::July, 21).unwrap()
        );
        let val: time::Date =
            AsValue::try_from_value(Value::Varchar(Some("2025-01-22".into()))).unwrap();
        assert_eq!(
            val,
            time::Date::from_calendar_date(2025, Month::January, 22).unwrap()
        );
        time::Date::try_from_value("1999-12-12error".into())
            .expect_err("Should not be able to convert wrong string");
    }

    #[test]
    fn value_time() {
        let var = time::Time::from_hms(0, 57, 21).unwrap();
        let val: Value = var.as_value();
        assert_eq!(val, Value::Time(Some(var)));
        assert_ne!(val, Value::Null);
        let var: time::Time = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Time = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, time::Time::from_hms(0, 57, 21).unwrap());
        assert_eq!(
            time::Time::try_from_value(Value::Varchar(Some("13:22".into()))).unwrap(),
            time::Time::from_hms(13, 22, 0).unwrap()
        );
    }

    #[test]
    fn value_datetime() {
        let var = time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
            time::Time::from_hms(13, 52, 13).unwrap(),
        );
        let val: Value = var.as_value();
        assert_eq!(val, Value::Timestamp(Some(var)));
        assert_ne!(val, Value::Varchar(None));
        let var: time::PrimitiveDateTime = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::PrimitiveDateTime = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(13, 52, 13).unwrap(),
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29T14:52:36.500".into())))
                .unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms_milli(14, 52, 36, 500).unwrap()
            )
        );
        assert_ne!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29T14:52:36".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52:36.500".into())))
                .unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms_milli(14, 52, 36, 500).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52:36".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 00).unwrap()
            )
        );
    }

    #[test]
    fn value_datetime_timezone() {
        let var = time::OffsetDateTime::new_in_offset(
            time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
            time::Time::from_hms(00, 35, 12).unwrap(),
            time::UtcOffset::from_hms(2, 0, 0).unwrap(),
        );
        let val: Value = var.as_value();
        assert_eq!(val, Value::TimestampWithTimezone(Some(var)));
        assert_ne!(val, Value::Date(Some(var.date())));

        assert_ne!(
            val,
            Value::TimestampWithTimezone(
                time::OffsetDateTime::new_in_offset(
                    time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                    time::Time::from_hms(00, 35, 12).unwrap(),
                    time::UtcOffset::from_hms(1, 0, 0).unwrap(),
                )
                .into()
            )
        );
        let var: time::OffsetDateTime = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::OffsetDateTime = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(00, 35, 12).unwrap(),
                time::UtcOffset::from_hms(2, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12.123+01:00".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms_milli(0, 35, 12, 123).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12.123+01".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms_milli(0, 35, 12, 123).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12+01:00".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 12).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12+01".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 12).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35+01:00".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 0).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35+01".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 0).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
    }

    #[test]
    fn value_interval() {
        let var = Interval::from_months(4);
        let val: Value = var.as_value();
        assert_eq!(val, Interval::from_months(4).as_value());
        assert_ne!(val, Interval::from_months(3).as_value());
        assert_ne!(val, Interval::from_days(28).as_value());
        let var: Interval = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Interval = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, Interval::from_months(4));
        assert_eq!(
            Interval::parse("1 year 2 mons").expect("Could not parse the interval"),
            Interval::from_years(1) + Interval::from_months(2),
        );
        assert_eq!(
            Interval::parse("-100 year -12 mons +3 days -04:05:06")
                .expect("Could not parse the interval"),
            Interval::from_years(-101)
                + Interval::from_days(3)
                + Interval::from_hours(-4)
                + Interval::from_mins(-5)
                + Interval::from_secs(-6),
        );
        assert_eq!(
            Interval::parse("2years 60days").expect("Could not parse the interval"),
            Interval::from_years(2) + Interval::from_days(60),
        );
        assert_eq!(
            Interval::parse("'1 year 2 mons 3 days 04:05:06.789'").unwrap(),
            Interval::from_years(1)
                + Interval::from_months(2)
                + Interval::from_days(3)
                + Interval::from_hours(4)
                + Interval::from_mins(5)
                + Interval::from_secs(6)
                + Interval::from_micros(789_000)
        );
        assert_eq!(
            Interval::parse("'2 years 1 mon 5 days 12:00:00.000000123'").unwrap(),
            Interval::from_years(2)
                + Interval::from_months(1)
                + Interval::from_days(5)
                + Interval::from_hours(12)
                + Interval::from_nanos(123)
        );
        assert_eq!(
            Interval::parse("'-1 year 2 mons -3 days 04:05:06.001002003'").unwrap(),
            Interval::from_years(-1)
                + Interval::from_months(2)
                + Interval::from_days(-3)
                + Interval::from_hours(4)
                + Interval::from_mins(5)
                + Interval::from_secs(6)
                + Interval::from_micros(1_002)
                + Interval::from_nanos(3)
        );
        assert_eq!(
            Interval::parse("-04:05:06.000123").unwrap(),
            Interval::from_hours(-4)
                + Interval::from_mins(-5)
                + Interval::from_secs(-6)
                + Interval::from_micros(-123)
        );
        assert_eq!(
            Interval::parse(
                "3 years 4 months 5 days 6 hours 7 minutes 8 seconds 9 microseconds 10 nanoseconds"
            )
            .unwrap(),
            Interval::from_years(3)
                + Interval::from_months(4)
                + Interval::from_days(5)
                + Interval::from_hours(6)
                + Interval::from_mins(7)
                + Interval::from_secs(8)
                + Interval::from_micros(9)
                + Interval::from_nanos(10)
        );
        assert_eq!(
            Interval::parse("2 Y 3 MONS 4 d 5 H 6 MIN 7 S 8 MICRO 9 NS").unwrap(),
            Interval::from_years(2)
                + Interval::from_months(3)
                + Interval::from_days(4)
                + Interval::from_hours(5)
                + Interval::from_mins(6)
                + Interval::from_secs(7)
                + Interval::from_micros(8)
                + Interval::from_nanos(9)
        );
        assert_eq!(
            Interval::parse("10:11:12.123456789").unwrap(),
            Interval::from_hours(10)
                + Interval::from_mins(11)
                + Interval::from_secs(12)
                + Interval::from_micros(123_456)
                + Interval::from_nanos(789)
        );
        assert_eq!(
            Interval::parse("1 year 12 months").unwrap(),
            Interval::from_years(2)
        );
        assert!(Interval::parse("5 HORS").is_err());
        assert!(Interval::parse("04:").is_err());
        assert!(Interval::parse("04:05:").is_err());
        assert!(Interval::parse("04:05:06.").is_err());
        assert!(Interval::parse("'2 days 01:02:03.0040050068473'   more").is_err());
        assert!(Interval::parse("'2 days\"").is_err());
        assert!(Interval::parse("'2 days 01:02:03.0\"").is_err());
        assert_eq!(
            Interval::parse("'2 days 01:02:03.004005006'").unwrap(),
            Interval::from_days(2)
                + Interval::from_hours(1)
                + Interval::from_mins(2)
                + Interval::from_secs(3)
                + Interval::from_micros(4_005)
                + Interval::from_nanos(6)
        );
    }

    #[test]
    fn value_time_duration() {
        let var = time::Duration::days(14);
        let val: Value = var.as_value();
        assert_eq!(val, Interval::from_days(14).as_value());
        assert_ne!(val, Interval::from_days(15).as_value());
        assert_ne!(val, Interval::from_secs(1).as_value());
        let var: time::Duration = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Duration = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, time::Duration::days(14));
    }

    #[test]
    fn value_std_duration() {
        let days_5 = std::time::Duration::new((5 * Interval::SECS_IN_DAY) as u64, 0);
        let days_1 = std::time::Duration::new((1 * Interval::SECS_IN_DAY) as u64, 0);
        let var = days_5.clone();
        let val: Value = var.as_value();
        assert_eq!(val, days_5.clone().as_value());
        assert_ne!(val, days_1.as_value());

        let var: std::time::Duration = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: std::time::Duration = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, days_5.clone());
    }

    #[test]
    fn value_uuid() {
        let var = Uuid::nil();
        let val: Value = var.as_value();
        assert_eq!(
            val,
            Uuid::parse_str("00000000-0000-0000-0000-000000000000")
                .unwrap()
                .as_value()
        );
        assert_ne!(
            val,
            Uuid::parse_str("10000000-0000-0000-0000-000000000000")
                .unwrap()
                .as_value()
        );
        assert_ne!(val, 5.as_value());

        let var: Uuid = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Uuid = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap()
        );

        let var = Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84")
            .unwrap()
            .as_value();
        let val: Value = var.as_value();
        assert_eq!(
            val,
            Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84")
                .unwrap()
                .as_value()
        );
        assert_ne!(
            val,
            Uuid::parse_str("80ae6ccb-2504-4d2e-b496-5d9759199625")
                .unwrap()
                .as_value()
        );
        assert_ne!(val, 5.as_value());

        let var: Uuid = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Uuid = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84").unwrap()
        );
        assert_eq!(
            Uuid::parse_str("6ed80631-c3ec-41a5-9f66-d9c1e5532798").unwrap(),
            Uuid::try_from_value("6ed80631-c3ec-41a5-9f66-d9c1e5532798".into()).unwrap()
        );
    }

    #[test]
    fn value_decimal() {
        let var = Decimal::from_i128_with_scale(12345, 2);
        let val: Value = var.as_value();
        assert_eq!(val, Decimal::from_f64(123.45).unwrap().as_value());
        assert_ne!(val, Decimal::from_f64(123.10).unwrap().as_value());
        let var: Decimal = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Decimal = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, Decimal::from_f64(123.45).unwrap());
        assert_eq!(
            Decimal::try_from_value(127_i8.as_value()).unwrap(),
            Decimal::from_f64(127.0).unwrap()
        );
        assert_ne!(
            Decimal::try_from_value(126_i8.as_value()).unwrap(),
            Decimal::from_f64(127.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value(0_i16.as_value()).unwrap(),
            Decimal::from_f64(0.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((-2147483648_i32).as_value()).unwrap(),
            Decimal::from_f64(-2147483648.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value(82664_i64.as_value()).unwrap(),
            Decimal::from_f64(82664.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((255_u8).as_value()).unwrap(),
            Decimal::from_f64(255.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((10000_u16).as_value()).unwrap(),
            Decimal::from_f64(10000.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((777_u32).as_value()).unwrap(),
            Decimal::from_f64(777.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((2_u32).as_value()).unwrap(),
            Decimal::from_f64(2.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((0_u64).as_value()).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            Decimal::try_from_value((4.25_f32).as_value()).unwrap(),
            Decimal::from_f64(4.25).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((-11.29_f64).as_value()).unwrap(),
            Decimal::from_f64(-11.29).unwrap()
        );
        Decimal::try_from_value("hello".into()).expect_err("Cannot convert a string to decimal");
        assert_eq!(Decimal::as_empty_value(), Value::Decimal(None, 0, 0));
        assert_ne!(
            Decimal::as_empty_value(),
            Value::Decimal(Some(Decimal::zero()), 0, 0)
        );
        assert_ne!(Decimal::as_empty_value(), Value::Decimal(None, 1, 0));
        assert_ne!(Decimal::as_empty_value(), Value::Decimal(None, 0, 1));
    }
}
