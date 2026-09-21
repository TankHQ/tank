#[cfg(test)]
mod tests {
    use tank_core::{Context, DynQuery, Fragment, SqlValueWriter};
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

    #[test]
    fn binary_is_written_as_hex_literal() {
        let writer = MySQLSqlWriter::default();
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlInsertIntoValues, false);
        // MySQL binary literals are `X'..'`; the generic `'\x..'` form is read as a
        // string and loses the raw bytes.
        writer.write_blob(&mut ctx, &mut out, &[0x00, 0xFF, 0xDE, 0xAD]);
        assert_eq!(out.as_str(), "X'00FFDEAD'");
    }
}
