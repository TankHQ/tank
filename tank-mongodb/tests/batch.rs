#[cfg(test)]
mod tests {
    use tank::{expr, Driver, DynQuery, Entity, SqlWriter};
    use tank_mongodb::{BatchPayload, MongoDBDriver, Payload};
    use tank_tests::init_logs;

    const DRIVER: MongoDBDriver = MongoDBDriver {};

    #[derive(Entity)]
    #[tank(schema = "test_db", name = "items")]
    struct Item {
        #[tank(primary_key)]
        pub id: i64,
        pub name: String,
    }

    #[test]
    fn batch_preserves_all_payloads() {
        init_logs();
        let writer = DRIVER.sql_writer();
        let mut query = DynQuery::default();

        writer.write_delete::<Item>(&mut query, expr!(Item::id == 1));
        writer.write_insert(
            &mut query,
            [&Item {
                id: 2,
                name: "test".into(),
            }],
            false,
        );

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
    fn batch_three_operations() {
        init_logs();
        let writer = DRIVER.sql_writer();
        let mut query = DynQuery::default();

        writer.write_insert(
            &mut query,
            [&Item {
                id: 1,
                name: "first".into(),
            }],
            false,
        );
        writer.write_insert(
            &mut query,
            [&Item {
                id: 2,
                name: "second".into(),
            }],
            false,
        );
        writer.write_delete::<Item>(&mut query, expr!(Item::id == 1));

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
    fn batch_upsert_and_delete() {
        init_logs();
        let writer = DRIVER.sql_writer();
        let mut query = DynQuery::default();

        writer.write_insert(
            &mut query,
            [&Item {
                id: 1,
                name: "upserted".into(),
            }],
            true, // update = true -> upsert
        );
        writer.write_delete::<Item>(&mut query, expr!(Item::id == 99));

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
    fn drop_collection_replaces_previous() {
        init_logs();
        let writer = DRIVER.sql_writer();
        let mut query = DynQuery::default();

        writer.write_insert(
            &mut query,
            [&Item {
                id: 1,
                name: "will be dropped".into(),
            }],
            false,
        );
        writer.write_drop_table::<Item>(&mut query, true);

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
