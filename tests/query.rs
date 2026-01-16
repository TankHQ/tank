#[cfg(test)]
mod tests {
    use indoc::indoc;
    use tank::{DynQuery, Entity, GenericSqlWriter, QueryBuilder, SqlWriter, expr};

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn query_1() {
        #[derive(Entity)]
        struct MyTable {
            a: u32,
            b: i8,
            c: String,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select(MyTable::columns())
                .from(MyTable::table()),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "a", "b", "c"
                FROM "my_table";
            "#}
            .trim()
        );
    }

    #[test]
    fn query_2() {
        #[derive(Entity)]
        #[tank(name = "the table")]
        struct TheTable {
            #[tank(name = "the_column")]
            some_col: i128,
            second_column: Box<String>,
        }
        let mut sql = DynQuery::default();
        WRITER.write_select(
            &mut sql,
            &QueryBuilder::new()
                .select([TheTable::second_column, TheTable::some_col])
                .from(TheTable::table())
                .where_condition(expr!(
                    TheTable::second_column == "So%" as LIKE
                        || TheTable::some_col >= 0 && TheTable::some_col < 10
                )),
        );
        assert_eq!(
            sql.as_str(),
            indoc! {r#"
                SELECT "second_column", "the_column"
                FROM "the table"
                WHERE "second_column" LIKE 'So%' OR "the_column" >= 0 AND "the_column" < 10;
            "#}
            .trim()
        );
    }
}
