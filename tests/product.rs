#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rust_decimal::Decimal;
    use std::borrow::Cow;
    use tank::{
        DefaultValueType, DynQuery, Entity, GenericSqlWriter, Passive, PrimaryKeyType, SqlWriter,
        TableRef, Value,
    };
    use time::{Date, Month, PrimitiveDateTime, Time};

    #[derive(Entity)]
    #[tank(name = "products")]
    struct Product {
        #[tank(primary_key)]
        id: Passive<u64>,
        name: String,
        price: Decimal,
        available: bool,
        tags: Vec<String>,
        added_on: PrimitiveDateTime,
    }

    impl Product {
        pub fn sample() -> Self {
            Self {
                id: Passive::NotSet,
                name: "Smartphone".into(),
                price: Decimal::new(49999, 2),
                available: true,
                tags: vec!["electronics".into(), "mobile".into()],
                added_on: PrimitiveDateTime::new(
                    Date::from_calendar_date(2025, Month::June, 24).unwrap(),
                    Time::from_hms(10, 30, 07).unwrap(),
                ),
            }
        }
    }
    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn test_product() {
        assert!(matches!(
            Product::table(),
            TableRef {
                name: Cow::Borrowed("products"),
                schema: Cow::Borrowed(""),
                alias: Cow::Borrowed(""),
            }
        ));
        assert_eq!(
            Product::primary_key_def()
                .iter()
                .map(|c| &c.column_ref.name)
                .collect::<Vec<_>>(),
            ["id"]
        );
        let columns = Product::columns();
        assert_eq!(columns.len(), 6);
        assert_eq!(columns[0].column_ref.name, "id");
        assert_eq!(columns[1].column_ref.name, "name");
        assert_eq!(columns[2].column_ref.name, "price");
        assert_eq!(columns[3].column_ref.name, "available");
        assert_eq!(columns[4].column_ref.name, "tags");
        assert_eq!(columns[5].column_ref.name, "added_on");
        assert_eq!(columns[0].column_ref.table, "products");
        assert_eq!(columns[1].column_ref.table, "products");
        assert_eq!(columns[2].column_ref.table, "products");
        assert_eq!(columns[3].column_ref.table, "products");
        assert_eq!(columns[4].column_ref.table, "products");
        assert_eq!(columns[5].column_ref.table, "products");
        assert_eq!(columns[0].column_ref.schema, "");
        assert_eq!(columns[1].column_ref.schema, "");
        assert_eq!(columns[2].column_ref.schema, "");
        assert_eq!(columns[3].column_ref.schema, "");
        assert_eq!(columns[4].column_ref.schema, "");
        assert_eq!(columns[5].column_ref.schema, "");
        assert!(matches!(columns[0].value, Value::UInt64(None, ..)));
        assert!(matches!(columns[1].value, Value::Varchar(None, ..)));
        assert!(matches!(columns[2].value, Value::Decimal(None, ..)));
        assert!(matches!(columns[3].value, Value::Boolean(None, ..)));
        assert!(matches!(columns[4].value, Value::List(None, ..)));
        assert!(matches!(columns[5].value, Value::Timestamp(None, ..)));
        assert_eq!(columns[0].nullable, false);
        assert_eq!(columns[1].nullable, false);
        assert_eq!(columns[2].nullable, false);
        assert_eq!(columns[3].nullable, false);
        assert_eq!(columns[4].nullable, false);
        assert_eq!(columns[5].nullable, false);
        assert!(matches!(columns[0].default, DefaultValueType::None));
        assert!(matches!(columns[1].default, DefaultValueType::None));
        assert!(matches!(columns[2].default, DefaultValueType::None));
        assert!(matches!(columns[3].default, DefaultValueType::None));
        assert!(matches!(columns[4].default, DefaultValueType::None));
        assert!(matches!(columns[5].default, DefaultValueType::None));
        assert_eq!(columns[0].primary_key, PrimaryKeyType::PrimaryKey);
        assert_eq!(columns[1].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[2].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[3].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[4].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[5].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[0].unique, false);
        assert_eq!(columns[1].unique, false);
        assert_eq!(columns[2].unique, false);
        assert_eq!(columns[3].unique, false);
        assert_eq!(columns[4].unique, false);
        assert_eq!(columns[5].unique, false);
        assert_eq!(columns[0].references, None);
        assert_eq!(columns[1].references, None);
        assert_eq!(columns[2].references, None);
        assert_eq!(columns[3].references, None);
        assert_eq!(columns[4].references, None);
        assert_eq!(columns[5].references, None);
        assert_eq!(columns[0].on_delete, None);
        assert_eq!(columns[1].on_delete, None);
        assert_eq!(columns[2].on_delete, None);
        assert_eq!(columns[3].on_delete, None);
        assert_eq!(columns[4].on_delete, None);
        assert_eq!(columns[5].on_delete, None);
        assert_eq!(columns[0].on_update, None);
        assert_eq!(columns[1].on_update, None);
        assert_eq!(columns[2].on_update, None);
        assert_eq!(columns[3].on_update, None);
        assert_eq!(columns[4].on_update, None);
        assert_eq!(columns[5].on_update, None);
        assert_eq!(columns[0].passive, true);
        assert_eq!(columns[1].passive, false);
        assert_eq!(columns[2].passive, false);
        assert_eq!(columns[3].passive, false);
        assert_eq!(columns[4].passive, false);
        assert_eq!(columns[5].passive, false);
    }

    #[test]
    fn test_product_create_table() {
        let mut query = DynQuery::default();
        WRITER.write_create_table::<Product>(&mut query, false);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                CREATE TABLE "products" (
                "id" UBIGINT PRIMARY KEY,
                "name" VARCHAR NOT NULL,
                "price" DECIMAL NOT NULL,
                "available" BOOLEAN NOT NULL,
                "tags" VARCHAR[] NOT NULL,
                "added_on" TIMESTAMP NOT NULL);
            "#}
            .trim()
        );
    }

    #[test]
    fn test_product_insert() {
        let mut query = DynQuery::default();
        WRITER.write_insert(&mut query, [&Product::sample()], false);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                INSERT INTO "products" ("name", "price", "available", "tags", "added_on") VALUES
                ('Smartphone', 499.99, true, ['electronics','mobile'], '2025-06-24T10:30:07.0');
            "#}
            .trim()
        );
    }

    #[test]
    fn test_product_insert_multiple() {
        let mut query = DynQuery::default();
        WRITER.write_insert(
            &mut query,
            [
                Product {
                    id: 74.into(),
                    name: "Headphones".into(),
                    price: Decimal::new(12995, 2),
                    available: false,
                    tags: vec!["electronics".into(), "audio".into()],
                    added_on: PrimitiveDateTime::new(
                        Date::from_calendar_date(2025, Month::July, 8).unwrap(),
                        Time::from_hms(14, 15, 01).unwrap(),
                    ),
                },
                Product::sample(),
                Product {
                    id: Passive::NotSet,
                    name: "Mouse".into(),
                    price: Decimal::new(3999, 2),
                    available: true,
                    tags: vec!["electronics".into(), "accessories".into()],
                    added_on: PrimitiveDateTime::new(
                        Date::from_calendar_date(2025, Month::July, 9).unwrap(),
                        Time::from_hms(9, 45, 30).unwrap(),
                    ),
                },
            ]
            .iter(),
            false,
        );
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                INSERT INTO "products" ("id", "name", "price", "available", "tags", "added_on") VALUES
                (74, 'Headphones', 129.95, false, ['electronics','audio'], '2025-07-08T14:15:01.0'),
                (DEFAULT, 'Smartphone', 499.99, true, ['electronics','mobile'], '2025-06-24T10:30:07.0'),
                (DEFAULT, 'Mouse', 39.99, true, ['electronics','accessories'], '2025-07-09T09:45:30.0');
            "#}
            .trim()
        );
    }
}
