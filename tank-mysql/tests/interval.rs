#[cfg(test)]
mod tests {
    use tank_core::{AsValue, Context, DynQuery, Fragment, Interval, SqlValueWriter};
    use tank_mysql::MySQLSqlWriter;

    fn render(interval: Interval) -> String {
        let writer = MySQLSqlWriter::default();
        let mut out = DynQuery::default();
        writer.write_value(
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
            &interval.as_value(),
        );
        out.as_str().to_string()
    }

    #[test]
    fn negative_subhour_interval_keeps_its_sign() {
        assert_eq!(render(-Interval::from_mins(30)), "'-00:30:00.0'");
        assert_eq!(render(-Interval::from_secs(1)), "'-00:00:01.0'");
        assert_eq!(render(-Interval::from_millis(500)), "'-00:00:00.5'");
        // Positive values are unchanged.
        assert_eq!(render(Interval::from_mins(30)), "'00:30:00.0'");
    }

    #[test]
    fn mixed_sign_interval_renders_consistently() {
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
}
