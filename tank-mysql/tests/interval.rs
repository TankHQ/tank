#[cfg(test)]
mod tests {
    use tank::{AnyQuery, AsValue, Context, Fragment, Interval, SqlValueWriter, describe_url};
    use tank_mysql::{MySQLDriver, MySQLSqlWriter};
    use tank_tests::init_logs;
    use url::Url;

    fn render(interval: Interval) -> String {
        let writer = MySQLSqlWriter::default();
        let mut out = AnyQuery::default();
        writer.write_value(
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
            &interval.as_value(),
        );
        out.as_str().to_string()
    }

    #[test]
    fn negative_subhour_sign() {
        init_logs();
        assert_eq!(render(-Interval::from_mins(30)), "'-00:30:00.0'");
        assert_eq!(render(-Interval::from_secs(1)), "'-00:00:01.0'");
        assert_eq!(render(-Interval::from_millis(500)), "'-00:00:00.5'");
        // Positive values are unchanged.
        assert_eq!(render(Interval::from_mins(30)), "'00:30:00.0'");
    }

    #[test]
    fn mixed_sign_interval() {
        init_logs();
        // -1 day + 30 minutes is -23:30, not -24:30.
        assert_eq!(
            render(Interval::from_days(-1) + Interval::from_mins(30)),
            "'-23:30:00.0'"
        );
        // 1 day - 30 minutes is +23:30.
        assert_eq!(
            render(Interval::from_days(1) - Interval::from_mins(30)),
            "'23:30:00.0'"
        );
    }

    #[test]
    fn describe_url_target() {
        init_logs();
        let url = Url::parse("mysql://user@db.example.com:3307/shop").unwrap();
        let target = describe_url::<MySQLDriver>(&url);
        assert_eq!(target, "db.example.com:3307/shop");
    }
}
