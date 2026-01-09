#[cfg(test)]
mod tests {
    use indoc::indoc;
    use std::{borrow::Cow, sync::Mutex};
    use tank::{
        DefaultValueType, Entity, GenericSqlWriter, PrimaryKeyType, SqlWriter, TableRef, Value,
        expr,
    };

    #[derive(Entity)]
    #[tank(name = "simple_entity", unique = ("a", Self::c), unique = (SomeSimpleEntity::b, "c"))]
    struct SomeSimpleEntity {
        a: i8,
        b: Option<String>,
        #[tank(unique)]
        c: Box<u16>,
        #[tank(ignore)]
        _d: Mutex<i32>,
    }

    impl SomeSimpleEntity {
        pub fn make_some() -> Self {
            Self {
                a: 40,
                b: Some("hello".into()),
                c: Box::new(777),
                _d: Mutex::new(123),
            }
        }
    }
    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn test_simple_entity() {
        assert!(matches!(
            SomeSimpleEntity::table(),
            TableRef {
                name: Cow::Borrowed("simple_entity"),
                schema: Cow::Borrowed(""),
                alias: Cow::Borrowed(""),
            }
        ));

        assert_eq!(SomeSimpleEntity::primary_key_def().len(), 0);

        let columns = SomeSimpleEntity::columns();
        assert_eq!(columns.len(), 3);
        assert!(matches!(columns[0].name(), "a"));
        assert!(matches!(columns[1].name(), "b"));
        assert!(matches!(columns[2].name(), "c"));
        assert!(matches!(columns[0].table(), "simple_entity"));
        assert!(matches!(columns[1].table(), "simple_entity"));
        assert!(matches!(columns[2].table(), "simple_entity"));
        assert!(matches!(columns[0].schema(), ""));
        assert!(matches!(columns[1].schema(), ""));
        assert!(matches!(columns[2].schema(), ""));
        assert!(matches!(columns[0].value, Value::Int8(..)));
        assert!(matches!(columns[1].value, Value::Varchar(..)));
        assert!(matches!(columns[2].value, Value::UInt16(..)));
        assert_eq!(columns[0].nullable, false);
        assert_eq!(columns[1].nullable, true);
        assert_eq!(columns[2].nullable, false);
        assert!(matches!(columns[0].default, DefaultValueType::None));
        assert!(matches!(columns[1].default, DefaultValueType::None));
        assert!(matches!(columns[2].default, DefaultValueType::None));
        assert_eq!(columns[0].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[1].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[2].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[0].unique, false);
        assert_eq!(columns[1].unique, false);
        assert_eq!(columns[2].unique, true);
        assert_eq!(columns[0].references, None);
        assert_eq!(columns[1].references, None);
        assert_eq!(columns[2].references, None);
        assert_eq!(columns[0].on_delete, None);
        assert_eq!(columns[1].on_delete, None);
        assert_eq!(columns[2].on_delete, None);
        assert_eq!(columns[0].on_update, None);
        assert_eq!(columns[1].on_update, None);
        assert_eq!(columns[2].on_update, None);
        assert_eq!(columns[0].passive, false);
        assert_eq!(columns[1].passive, false);
        assert_eq!(columns[2].passive, false);
    }

    #[test]
    fn test_simple_entity_create_table() {
        let mut query = String::new();
        WRITER.write_create_table::<SomeSimpleEntity>(&mut query, false);
        assert_eq!(
            query,
            indoc! {r#"
                CREATE TABLE "simple_entity" (
                "a" TINYINT NOT NULL,
                "b" VARCHAR,
                "c" USMALLINT NOT NULL UNIQUE,
                UNIQUE ("a", "c"),
                UNIQUE ("b", "c"));
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_drop_table() {
        let mut query = String::new();
        WRITER.write_drop_table::<SomeSimpleEntity>(&mut query, true);
        assert_eq!(query, r#"DROP TABLE IF EXISTS "simple_entity";"#);
    }

    #[test]
    fn test_simple_entity_select() {
        let mut query = String::new();
        WRITER.write_select(
            &mut query,
            SomeSimpleEntity::columns(),
            SomeSimpleEntity::table(),
            expr!(SomeSimpleEntity::a > 100),
            Some(1000),
        );
        assert_eq!(
            query,
            indoc! {r#"
                SELECT "a", "b", "c"
                FROM "simple_entity"
                WHERE "a" > 100
                LIMIT 1000;
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_insert() {
        let mut query = String::new();
        WRITER.write_insert(&mut query, [&SomeSimpleEntity::make_some()], true);
        assert_eq!(
            query,
            indoc! {r#"
                INSERT INTO "simple_entity" ("a", "b", "c") VALUES
                (40, 'hello', 777);
            "#}
            .trim()
        );
    }

    #[test]
    fn test_simple_entity_delete() {
        let mut query = String::new();
        WRITER.write_delete::<SomeSimpleEntity>(&mut query, expr!(SomeSimpleEntity::b != "hello"));
        assert_eq!(
            query,
            indoc! {r#"
                DELETE FROM "simple_entity"
                WHERE "b" != 'hello';
            "#}
            .trim()
        );
    }
}
