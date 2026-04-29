#[cfg(test)]
mod tests {
    use tank_core::{Context, DynQuery, Fragment, SqlWriter};
    use tank_mysql::MySQLSqlWriter;

    #[test]
    fn backslash_is_escaped_in_strings() {
        let writer = MySQLSqlWriter::default();
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelectWhere, false);
        // A backslash followed by a single quote: in MySQL default mode,
        // \' is interpreted as an escaped quote, so \ must be doubled.
        writer.write_string(&mut ctx, &mut out, "test\\' OR 1=1 --");
        let sql = out.as_str();
        // The backslash must be doubled, and the quote separately escaped
        // Expected: 'test\\'' OR 1=1 --'
        // - \\\\ = escaped backslash
        // - '' = escaped single quote
        assert_eq!(
            sql, "'test\\\\'' OR 1=1 --'",
            "Backslash and quote must both be escaped for MySQL"
        );
    }
}
