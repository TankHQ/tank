#[cfg(test)]
mod tests {
    use indoc::indoc;
    use std::borrow::Cow;
    use tank::{DynQuery, Entity, GenericSqlWriter, QueryBuilder, SqlWriter, TableRef};

    #[derive(Entity, Default)]
    #[tank(schema = "the_schema", name = "empty_entity")]
    struct SomeEmptyEntity {}

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn test_simple_entity() {
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

        let columns = SomeEmptyEntity::columns();
        assert_eq!(columns.len(), 0);
    }

    #[test]
    fn test_simple_entity_create_table() {
        let mut query = DynQuery::default();
        WRITER.write_create_table::<SomeEmptyEntity>(&mut query, true);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                CREATE TABLE IF NOT EXISTS "the_schema"."empty_entity" (
                );
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_drop_table() {
        let mut query = DynQuery::default();
        WRITER.write_drop_table::<SomeEmptyEntity>(&mut query, false);
        assert_eq!(query.as_str(), r#"DROP TABLE "the_schema"."empty_entity";"#);
    }

    #[test]
    fn test_simple_entity_select() {
        let mut query = DynQuery::default();
        WRITER.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(SomeEmptyEntity::columns())
                .from(SomeEmptyEntity::table()),
        );
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                SELECT *
                FROM "the_schema"."empty_entity";
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_insert() {
        let mut query = DynQuery::default();
        WRITER.write_insert(&mut query, [&SomeEmptyEntity::default()], true);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                INSERT INTO "the_schema"."empty_entity" () VALUES
                ();
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_delete() {
        let mut query = DynQuery::default();
        WRITER.write_delete::<SomeEmptyEntity>(&mut query, false);
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
