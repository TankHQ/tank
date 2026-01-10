#[cfg(test)]
mod tests {
    use std::{i64, time::Duration};
    use tank_core::{AsValue, Interval, RawQuery, SqlWriter};

    struct Writer;
    impl SqlWriter for Writer {
        fn as_dyn(&self) -> &dyn SqlWriter {
            self
        }
    }
    const WRITER: Writer = Writer {};

    macro_rules! test_interval {
        ($interval:expr, $expected:literal) => {{
            let mut out = RawQuery::default();
            WRITER.write_value(&mut Default::default(), &mut out, &$interval.as_value());
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
}
