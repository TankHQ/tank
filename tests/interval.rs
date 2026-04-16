#[cfg(test)]
mod tests {
    use std::{collections::HashSet, i64, time::Duration};
    use tank_core::{AsValue, Context, DynQuery, Fragment, Interval, SqlWriter};

    struct Writer;
    impl SqlWriter for Writer {
        fn as_dyn(&self) -> &dyn SqlWriter {
            self
        }
    }
    const WRITER: Writer = Writer {};

    macro_rules! test_interval {
        ($interval:expr, $expected:literal) => {{
            let mut out = DynQuery::default();
            WRITER.write_value(
                &mut Context::new(Fragment::SqlSelect, false),
                &mut out,
                &$interval.as_value(),
            );
            assert_eq!(out.as_str(), $expected);
        }};
    }

    #[test]
    fn sql() {
        test_interval!(Interval::default(), "INTERVAL '0 SECONDS'");
        test_interval!(Interval::from_nanos(1), "INTERVAL '1 NANOSECOND'");
        test_interval!(Interval::from_nanos(27), "INTERVAL '27 NANOSECONDS'");
        test_interval!(Interval::from_nanos(1_000), "INTERVAL '1 MICROSECOND'");
        test_interval!(Interval::from_nanos(54_000), "INTERVAL '54 MICROSECONDS'");
        test_interval!(
            Interval::from_nanos(864_000_000_000_000),
            "INTERVAL '10 DAYS'"
        );
        test_interval!(
            Interval::from_nanos(864_000_000_000_001),
            "INTERVAL '10 DAYS 1 NANOSECOND'"
        );
        test_interval!(
            Interval::from_nanos(864_000_000_000_010),
            "INTERVAL '10 DAYS 10 NANOSECONDS'"
        );
        test_interval!(
            Interval::from_nanos(864_000_000_001_000),
            "INTERVAL '10 DAYS 1 MICROSECOND'"
        );
        test_interval!(
            Interval::from_nanos(864_000_000_001_010),
            "INTERVAL '10 DAYS 1010 NANOSECONDS'"
        );

        test_interval!(Interval::from_micros(1), "INTERVAL '1 MICROSECOND'");
        test_interval!(
            Interval::from_duration(&std::time::Duration::from_micros(1)),
            "INTERVAL '1 MICROSECOND'"
        );
        test_interval!(Interval::from_micros(2), "INTERVAL '2 MICROSECONDS'");
        test_interval!(Interval::from_micros(999), "INTERVAL '999 MICROSECONDS'");
        test_interval!(Interval::from_micros(1_001), "INTERVAL '1001 MICROSECONDS'");
        test_interval!(Interval::from_micros(1_000_000), "INTERVAL '1 SECOND'");
        test_interval!(Interval::from_micros(2_000_000), "INTERVAL '2 SECONDS'");
        test_interval!(Interval::from_micros(3_000_000), "INTERVAL '3 SECONDS'");
        test_interval!(
            Interval::from_micros(1_000_999),
            "INTERVAL '1000999 MICROSECONDS'"
        );
        test_interval!(
            Interval::from_micros(1_001_000_000),
            "INTERVAL '1001 SECONDS'"
        );
        test_interval!(
            Interval::from_micros(1_012_000_000),
            "INTERVAL '1012 SECONDS'"
        );
        test_interval!(Interval::from_micros(3_600_000_000), "INTERVAL '1 HOUR'");
        test_interval!(Interval::from_micros(21_600_000_000), "INTERVAL '6 HOURS'");
        test_interval!(
            Interval::from_micros(21_600_000_001),
            "INTERVAL '6 HOURS 1 MICROSECOND'"
        );
        test_interval!(
            Interval::from_micros(3_110_400_000_000),
            "INTERVAL '36 DAYS'"
        );

        test_interval!(Interval::from_millis(1_000), "INTERVAL '1 SECOND'");
        test_interval!(Interval::from_millis(2_000), "INTERVAL '2 SECONDS'");
        test_interval!(Interval::from_millis(60_000), "INTERVAL '1 MINUTE'");
        test_interval!(Interval::from_millis(3_600_000), "INTERVAL '1 HOUR'");
        test_interval!(Interval::from_millis(86_400_000), "INTERVAL '1 DAY'");
        test_interval!(Interval::from_millis(172_800_000), "INTERVAL '2 DAYS'");

        test_interval!(Interval::from_mins(1), "INTERVAL '1 MINUTE'");
        test_interval!(Interval::from_mins(2), "INTERVAL '2 MINUTES'");
        test_interval!(Interval::from_mins(59), "INTERVAL '59 MINUTES'");
        test_interval!(Interval::from_mins(60), "INTERVAL '1 HOUR'");
        test_interval!(Interval::from_mins(61), "INTERVAL '61 MINUTES'");
        test_interval!(Interval::from_mins(90), "INTERVAL '90 MINUTES'");
        test_interval!(Interval::from_mins(120), "INTERVAL '2 HOURS'");
        test_interval!(Interval::from_mins(1_440), "INTERVAL '1 DAY'");
        test_interval!(Interval::from_mins(1_500), "INTERVAL '25 HOURS'");
        test_interval!(Interval::from_mins(2_880), "INTERVAL '2 DAYS'");
        test_interval!(Interval::from_mins(4_320), "INTERVAL '3 DAYS'");
        test_interval!(Interval::from_mins(10_080), "INTERVAL '7 DAYS'");
        test_interval!(Interval::from_mins(43_200), "INTERVAL '30 DAYS'");
        test_interval!(Interval::from_mins(525_600), "INTERVAL '365 DAYS'");
        test_interval!(Interval::from_mins(12_016_800), "INTERVAL '8345 DAYS'");

        test_interval!(Interval::from_days(1), "INTERVAL '1 DAY'");
        test_interval!(Interval::from_days(6_000_000), "INTERVAL '6000000 DAYS'");

        test_interval!(Interval::from_weeks(1), "INTERVAL '7 DAYS'");
        test_interval!(Interval::from_weeks(2), "INTERVAL '14 DAYS'");
        test_interval!(Interval::from_weeks(3), "INTERVAL '21 DAYS'");
        test_interval!(Interval::from_weeks(4), "INTERVAL '28 DAYS'");
        test_interval!(Interval::from_weeks(10), "INTERVAL '70 DAYS'");
        test_interval!(Interval::from_weeks(52), "INTERVAL '364 DAYS'");
        test_interval!(Interval::from_weeks(104), "INTERVAL '728 DAYS'");
        test_interval!(Interval::from_weeks(260), "INTERVAL '1820 DAYS'");
        test_interval!(Interval::from_weeks(1_000), "INTERVAL '7000 DAYS'");
        test_interval!(Interval::from_months(1), "INTERVAL '1 MONTH'");
        test_interval!(Interval::from_months(5), "INTERVAL '5 MONTHS'");

        test_interval!(Interval::from_days(-5), "INTERVAL '-5 DAYS'");
        test_interval!(Interval::from_months(-12), "INTERVAL '-1 YEARS'");
        test_interval!(Interval::from_months(-13), "INTERVAL '-13 MONTHS'");
        test_interval!(
            Interval::from_years(1) - Interval::from_days(3),
            "INTERVAL '1 YEAR -3 DAYS'"
        );
        test_interval!(
            Interval::from_days(3) - Interval::from_months(1),
            "INTERVAL '-1 MONTHS 3 DAYS'"
        );

        test_interval!(
            Interval {
                months: 12,
                days: 15,
                nanos: Interval::NANOS_IN_DAY * 2
            },
            "INTERVAL '1 YEAR 17 DAYS'"
        );
        test_interval!(
            Interval {
                months: 48,
                days: 15,
                nanos: Interval::NANOS_IN_DAY * 2 + 1_000_000_000
            },
            "INTERVAL '4 YEARS 1468801 SECONDS'"
        );
        test_interval!(
            Interval::from_years(5000) + Interval::from_months(1),
            "INTERVAL '5000 YEARS 1 MONTH'"
        );
        test_interval!(
            Interval::from_days(30) - Interval::from_days(1) + Interval::from_millis(10),
            "INTERVAL '29 DAYS 10000 MICROSECONDS'"
        );
        test_interval!(
            Interval::from_years(29)
                + Interval::from_months(12)
                + Interval::from_millis(10)
                + Interval::from_millis(990),
            "INTERVAL '30 YEARS 1 SECOND'"
        );
        test_interval!(
            Interval::from_months(5) + Interval::from_days(-2) + Interval::from_secs(1),
            "INTERVAL '5 MONTHS -172799 SECONDS'"
        );
    }

    #[test]
    fn operations() {
        let days_11 = Interval::from_days(10) + Interval::from_secs(86400);
        assert_ne!(
            days_11 + Interval::from_millis(1),
            Interval::from_millis(950_400_000)
        );
        assert_eq!(
            days_11 + Interval::from_millis(1),
            Interval::from_millis(950_400_001)
        );

        let almost_max_days = Interval::from_days(i64::MAX - 1);
        assert_eq!(
            almost_max_days + Interval::from_nanos(Interval::NANOS_IN_DAY),
            Interval {
                months: 0,
                days: i64::MAX,
                nanos: 0,
            }
        );
        assert_eq!(
            almost_max_days + Interval::from_nanos(Interval::NANOS_IN_DAY) + Interval::from_days(1),
            Interval {
                months: 0,
                days: i64::MAX,
                nanos: Interval::NANOS_IN_DAY,
            }
        );

        assert_eq!(
            Interval {
                months: 12,
                days: 45,
                nanos: Interval::NANOS_IN_DAY * 10 + 15,
            } + Interval::from_micros(1)
                - Interval {
                    months: 9,
                    days: 1,
                    nanos: Interval::NANOS_IN_DAY,
                },
            Interval {
                months: 3,   // 12 - 9
                days: 53,    // 45 + 10 - 1 - 1
                nanos: 1015, // 15 + 1000
            }
        );

        assert_eq!(
            Interval::from_years(5000) + Interval::from_months(1),
            Interval {
                months: 5000 * 12 + 1,
                ..Default::default()
            }
        )
    }

    #[test]
    fn conversion() {
        let value = time::Duration::minutes(1) + time::Duration::days(1);
        let expected: time::Duration = Interval::from_mins(1441).into();
        assert_eq!(value, expected);

        let value = Duration::from_micros(1) + Duration::from_hours(6);
        let expected: time::Duration = Interval::from_micros(1 + 6 * 3600 * 1_000_000).into();
        assert_eq!(value, expected);
    }

    #[test]
    fn from_bitmask() {
        use tank_core::IntervalUnit;
        assert_eq!(
            IntervalUnit::from_bitmask(1).unwrap(),
            IntervalUnit::Nanosecond
        );
        assert_eq!(
            IntervalUnit::from_bitmask(2).unwrap(),
            IntervalUnit::Microsecond
        );
        assert_eq!(IntervalUnit::from_bitmask(4).unwrap(), IntervalUnit::Second);
        assert_eq!(IntervalUnit::from_bitmask(8).unwrap(), IntervalUnit::Minute);
        assert_eq!(IntervalUnit::from_bitmask(16).unwrap(), IntervalUnit::Hour);
        assert_eq!(IntervalUnit::from_bitmask(32).unwrap(), IntervalUnit::Day);
        assert_eq!(IntervalUnit::from_bitmask(64).unwrap(), IntervalUnit::Month);
        assert_eq!(IntervalUnit::from_bitmask(128).unwrap(), IntervalUnit::Year);
        assert!(IntervalUnit::from_bitmask(0).is_err());
        assert!(IntervalUnit::from_bitmask(3).is_err());
        assert!(IntervalUnit::from_bitmask(255).is_err());
    }

    #[test]
    fn as_hmsns() {
        let interval = Interval::from_hours(2) + Interval::from_mins(30) + Interval::from_secs(15);
        let (h, m, s, ns) = interval.as_hmsns();
        assert_eq!(h, 2);
        assert_eq!(m, 30);
        assert_eq!(s, 15);
        assert_eq!(ns, 0);

        let interval2 = Interval::from_nanos(500);
        let (h, m, s, ns) = interval2.as_hmsns();
        assert_eq!(h, 0);
        assert_eq!(m, 0);
        assert_eq!(s, 0);
        assert_eq!(ns, 500);

        // With months and days contributing to hours
        let interval3 = Interval::new(1, 2, 0); // 1 month + 2 days
        let (h, _, _, _) = interval3.as_hmsns();
        assert_eq!(h, (1 * 30 + 2) * 24); // (months*30 + days) * 24
    }

    #[test]
    fn is_zero() {
        assert!(Interval::ZERO.is_zero());
        assert!(Interval::default().is_zero());
        assert!(!Interval::from_nanos(1).is_zero());
        assert!(!Interval::from_days(1).is_zero());
        assert!(!Interval::from_months(1).is_zero());
    }

    #[test]
    fn as_duration_method() {
        let interval = Interval::from_days(1);
        let duration = interval.as_duration(30.0);
        assert_eq!(duration, Duration::from_secs(86400));

        let interval2 = Interval::from_months(1);
        let duration2 = interval2.as_duration(30.0);
        assert_eq!(duration2, Duration::from_secs(30 * 86400));

        let interval3 = Interval::from_secs(1) + Interval::from_nanos(500);
        let duration3 = interval3.as_duration(30.0);
        assert_eq!(duration3, Duration::new(1, 500));
    }

    #[test]
    fn units_mask_and_unit_value() {
        let interval = Interval::from_years(2);
        let mask = interval.units_mask();
        assert_eq!(mask, 1 << 7); // Year bit
        assert_eq!(interval.unit_value(tank_core::IntervalUnit::Year), 2);

        let interval2 = Interval::from_months(5);
        let mask = interval2.units_mask();
        assert_eq!(mask, 1 << 6); // Month bit
        assert_eq!(interval2.unit_value(tank_core::IntervalUnit::Month), 5);

        let interval3 = Interval::from_days(3);
        let mask = interval3.units_mask();
        assert_eq!(mask, 1 << 5); // Day bit
        assert_eq!(interval3.unit_value(tank_core::IntervalUnit::Day), 3);

        let interval4 = Interval::from_hours(6);
        let mask = interval4.units_mask();
        assert_eq!(mask, 1 << 4); // Hour bit
        assert_eq!(interval4.unit_value(tank_core::IntervalUnit::Hour), 6);

        let interval5 = Interval::from_mins(45);
        let mask = interval5.units_mask();
        assert_eq!(mask, 1 << 3); // Minute bit
        assert_eq!(interval5.unit_value(tank_core::IntervalUnit::Minute), 45);

        let interval6 = Interval::from_secs(10);
        let mask = interval6.units_mask();
        assert_eq!(mask, 1 << 2); // Second bit
        assert_eq!(interval6.unit_value(tank_core::IntervalUnit::Second), 10);

        let interval7 = Interval::from_micros(500);
        let mask = interval7.units_mask();
        assert_eq!(mask, 1 << 1); // Microsecond bit
        assert_eq!(
            interval7.unit_value(tank_core::IntervalUnit::Microsecond),
            500
        );

        let interval8 = Interval::from_nanos(42);
        let mask = interval8.units_mask();
        assert_eq!(mask, 1 << 0); // Nanosecond bit
        assert_eq!(
            interval8.unit_value(tank_core::IntervalUnit::Nanosecond),
            42
        );

        assert_eq!(Interval::ZERO.units_mask(), 0);
    }

    #[test]
    fn hash_interval() {
        let mut set = HashSet::new();
        set.insert(Interval::from_days(1));
        set.insert(Interval::from_days(1));
        assert_eq!(set.len(), 1);
        set.insert(Interval::from_days(2));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn add_assign_sub_assign() {
        let mut interval = Interval::from_days(5);
        interval += Interval::from_days(3);
        assert_eq!(interval, Interval::from_days(8));

        interval -= Interval::from_days(2);
        assert_eq!(interval, Interval::from_days(6));
    }

    #[test]
    fn neg_interval() {
        let interval = Interval::from_days(3);
        let negative = -interval;
        assert_eq!(negative, Interval::from_days(-3));

        let interval2 = Interval::new(2, 5, 1000);
        let negative2 = -interval2;
        assert_eq!(negative2.months, -2);
        assert_eq!(negative2.days, -5);
        assert_eq!(negative2.nanos, -1000);
    }

    #[test]
    fn from_std_duration() {
        let duration = Duration::from_secs(90000); // 1 day + 3600 secs
        let interval: Interval = duration.into();
        assert_eq!(interval.days, 1);
        assert_eq!(interval.nanos, 3600 * Interval::NANOS_IN_SEC);
        assert_eq!(interval.months, 0);

        let d2: Duration = interval.into();
        assert_eq!(duration, d2);
    }

    #[test]
    fn from_time_duration() {
        let duration = time::Duration::seconds(90000); // 1 day + 3600 s
        let interval: Interval = duration.into();
        assert_eq!(interval.days, 1);
        assert_eq!(interval.months, 0);

        let duration2: time::Duration = interval.into();
        assert_eq!(duration, duration2);
    }
}
