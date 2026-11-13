#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::DRIVER;
    use indoc::indoc;
    use std::borrow::Cow;
    use tank::{Entity, Sql, TableRef};

    #[derive(Entity, Default)]
    #[tank(schema = "the_schema", name = "empty_entity")]
    struct SomeEmptyEntity {}

    #[test]
    fn empty_entity_sql() {
        assert!(matches!(
            SomeEmptyEntity::table(),
            TableRef {
                name: Cow::Borrowed("empty_entity"),
                schema: Cow::Borrowed("the_schema"),
                alias: Cow::Borrowed(""),
                ..
            }
        ));
        assert_eq!(SomeEmptyEntity::primary_key_def().len(), 0);
        assert_eq!(SomeEmptyEntity::columns().len(), 0);

        let query = Sql()
            .create_table(SomeEmptyEntity::table())
            .if_not_exists()
            .build(&DRIVER);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                CREATE TABLE IF NOT EXISTS "the_schema"."empty_entity" (
                );
            "#}
            .trim()
        );

        let query = Sql().drop_table(SomeEmptyEntity::table()).build(&DRIVER);
        assert_eq!(query.as_str(), r#"DROP TABLE "the_schema"."empty_entity";"#);

        let query = Sql()
            .select(SomeEmptyEntity::columns())
            .from(SomeEmptyEntity::table())
            .build(&DRIVER);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                SELECT *
                FROM "the_schema"."empty_entity";
            "#}
            .trim()
        );

        let query = Sql()
            .insert_into(SomeEmptyEntity::table())
            .values(&[SomeEmptyEntity::default()])
            .update()
            .build(&DRIVER);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                INSERT INTO "the_schema"."empty_entity" () VALUES
                ();
            "#}
            .trim()
        );

        let query = Sql()
            .delete_from(SomeEmptyEntity::table())
            .where_expr(false)
            .build(&DRIVER);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                DELETE FROM "the_schema"."empty_entity"
                WHERE false;
            "#}
            .trim()
        );
    }
}
