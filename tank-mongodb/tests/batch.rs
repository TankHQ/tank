#[cfg(test)]
mod tests {
    use tank::{AnyQuery, AsValue, Driver, Entity, Sql, Value, cols, expr};
    use tank_mongodb::{BatchPayload, InsertManyPayload, MongoDBDriver, Payload};
    use tank_tests::{init_logs, silent_logs};

    const DRIVER: MongoDBDriver = MongoDBDriver {};

    #[derive(Entity)]
    #[tank(schema = "test_db", name = "items")]
    struct Item {
        #[tank(primary_key)]
        pub id: i64,
        pub name: String,
    }

    #[test]
    fn batch_preserves_payloads() {
        init_logs();
        let mut query = AnyQuery::default();
        Sql()
            .delete_from(Item::table())
            .where_expr(expr!(Item::id == 1))
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .insert_into(Item::table())
            .values(&[Item {
                id: 2,
                name: "test".into(),
            }])
            .write_into(&DRIVER.sql_writer(), &mut query);

        let Some(Payload::Batch(BatchPayload { batch, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected batch payload");
        };
        assert_eq!(batch.len(), 2, "Batch should contain both payloads");
        assert!(matches!(batch[0], Payload::Delete(..)));
        assert!(matches!(batch[1], Payload::InsertOne(..)));
    }

    #[test]
    fn insert_plain_values() {
        init_logs();
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(Item::table())
            .columns(["id", "name"])
            .values([
                [1_i64.as_value(), "a".as_value()],
                [2_i64.as_value(), "b".as_value()],
            ])
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .insert_into(Item::table())
            .columns(["id"])
            .values([[3_i64], [4_i64]])
            .write_into(&DRIVER.sql_writer(), &mut query);
        let Some(Payload::Batch(BatchPayload { batch, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected batch payload");
        };
        let values = batch
            .iter()
            .flat_map(|payload| match payload {
                Payload::InsertMany(InsertManyPayload { rows, .. }) => rows.iter(),
                _ => panic!("Expected insert many payloads"),
            })
            .map(|row| row.values.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [
                vec![Value::Int64(Some(1)), Value::Varchar(Some("a".into()))],
                vec![Value::Int64(Some(2)), Value::Varchar(Some("b".into()))],
                vec![Value::Int64(Some(3))],
                vec![Value::Int64(Some(4))],
            ]
        );

        // Parameters need a database to evaluate, nothing is written.
        let mut query = AnyQuery::default();
        silent_logs! {
            Sql()
                .insert_into(Item::table())
                .columns(["id", "name"])
                .values([cols!(?, ?)])
                .write_into(&DRIVER.sql_writer(), &mut query);
        }
        assert!(query.is_empty());
    }

    #[test]
    fn batch_three_operations() {
        init_logs();
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(Item::table())
            .values(&[Item {
                id: 1,
                name: "first".into(),
            }])
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .insert_into(Item::table())
            .values(&[Item {
                id: 2,
                name: "second".into(),
            }])
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .delete_from(Item::table())
            .where_expr(expr!(Item::id == 1))
            .write_into(&DRIVER.sql_writer(), &mut query);

        let Some(Payload::Batch(BatchPayload { batch, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected batch payload");
        };
        assert_eq!(batch.len(), 3, "Batch should contain all three payloads");
        assert!(matches!(batch[0], Payload::InsertOne(..)));
        assert!(matches!(batch[1], Payload::InsertOne(..)));
        assert!(matches!(batch[2], Payload::Delete(..)));
    }

    #[test]
    fn batch_upsert_delete() {
        init_logs();
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(Item::table())
            .values(&[Item {
                id: 1,
                name: "upserted".into(),
            }])
            .update()
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .delete_from(Item::table())
            .where_expr(expr!(Item::id == 99))
            .write_into(&DRIVER.sql_writer(), &mut query);

        let Some(Payload::Batch(BatchPayload { batch, .. })) = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
        else {
            panic!("Expected batch payload");
        };
        assert_eq!(batch.len(), 2, "Batch should contain upsert + delete");
        assert!(matches!(batch[0], Payload::Upsert(..)));
        assert!(matches!(batch[1], Payload::Delete(..)));
    }

    #[test]
    fn drop_collection_replaces() {
        init_logs();
        let mut query = AnyQuery::default();
        Sql()
            .insert_into(Item::table())
            .values(&[Item {
                id: 1,
                name: "will be dropped".into(),
            }])
            .write_into(&DRIVER.sql_writer(), &mut query);
        Sql()
            .drop_table(Item::table())
            .if_exists()
            .write_into(&DRIVER.sql_writer(), &mut query);
        let payload = query
            .as_prepared::<MongoDBDriver>()
            .map(|v| v.get_payload())
            .expect("Expected a prepared query");

        assert!(
            matches!(payload, Payload::DropCollection(..)),
            "Drop should replace previous insert since they target the same collection"
        );
    }
}
