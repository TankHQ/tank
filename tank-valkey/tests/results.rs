mod init;

#[cfg(test)]
mod tests {
    use super::init::init_valkey;
    use std::{pin::pin, sync::Mutex};
    use tank::{
        Entity, Executor, QueryBuilder, QueryResult, Row, cols, expr, stream::StreamExt,
    };
    use tank_core::Connection;
    use tank_valkey::{ValkeyConnection, ValkeyDriver};
    use testcontainers_modules::testcontainers::ContainerAsync;
    use testcontainers_modules::valkey::Valkey;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[derive(Entity, Debug, PartialEq, Clone)]
    #[tank(schema = "namespace", primary_key = (id,))]
    struct Item {
        id: i64,
        name: String,
    }

    async fn connect() -> (ValkeyConnection, Option<ContainerAsync<Valkey>>) {
        let (url, container) = init_valkey(false).await;
        let connection = ValkeyConnection::connect(&ValkeyDriver::default(), url.into())
            .await
            .unwrap();
        (connection, container)
    }

    /// `SELECT *` on a missing hash is an empty rowset, not an error.
    #[tokio::test]
    async fn select_star_missing_is_an_empty_row_result() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let query = QueryBuilder::new()
            .select(cols!(*))
            .from(Item::table())
            .where_expr(expr!(Item::id == 999_999))
            .build(&ValkeyDriver::default());
        let mut results = pin!(connection.run(query));
        assert!(results.next().await.is_none());
    }

    /// `SELECT *` on a present hash decodes to exactly one row.
    #[tokio::test]
    async fn select_star_decodes_present_hash_as_one_row() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        Item {
            id: 7,
            name: "stored".into(),
        }
        .save(&mut connection)
        .await
        .unwrap();

        {
            let query = QueryBuilder::new()
                .select(cols!(*))
                .from(Item::table())
                .where_expr(expr!(Item::id == 7))
                .build(&ValkeyDriver::default());
            let mut results = pin!(connection.run(query));
            let Some(Ok(QueryResult::Row(row))) = results.next().await else {
                panic!("Expected Row");
            };
            assert!(row.names().iter().any(|name| name == "name"));
            assert_eq!(
                row.get_column("name").unwrap().as_ref(),
                &tank_core::Value::from("stored")
            );
            assert!(results.next().await.is_none());
        }

        Item::delete_many(&mut connection, expr!(Item::id == 7))
            .await
            .unwrap();
    }

    /// An explicit column read is a single result, including zero rows.
    #[tokio::test]
    async fn explicit_read_pipeline_is_one_result_including_zero_rows() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        Item {
            id: 7,
            name: "stored".into(),
        }
        .save(&mut connection)
        .await
        .unwrap();

        // Missing key: zero rows.
        {
            let query = QueryBuilder::new()
                .select(cols!(Item::id, Item::name))
                .from(Item::table())
                .where_expr(expr!(Item::id == 999_999))
                .build(&ValkeyDriver::default());
            let mut results = pin!(connection.run(query));
            assert!(results.next().await.is_none());
        }

        // Present key: one row with both columns.
        {
            let query = QueryBuilder::new()
                .select(cols!(Item::id, Item::name))
                .from(Item::table())
                .where_expr(expr!(Item::id == 7))
                .build(&ValkeyDriver::default());
            let mut results = pin!(connection.run(query));
            let Some(Ok(QueryResult::Row(row))) = results.next().await else {
                panic!("Expected Row");
            };
            assert_eq!(row.len(), 2);
            assert!(results.next().await.is_none());
        }

        Item::delete_many(&mut connection, expr!(Item::id == 7))
            .await
            .unwrap();
    }

    /// A write pipeline reports a single unknown effect, never an integer count.
    #[tokio::test]
    async fn write_pipeline_keeps_one_unknown_effect_instead_of_counting_integer_replies() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let affected = Item::insert_many(
            &mut connection,
            [Item {
                id: 7,
                name: "stored".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(affected.rows_affected, None);
        assert_eq!(affected.last_affected_id, None);

        Item::delete_many(&mut connection, expr!(Item::id == 7))
            .await
            .unwrap();
    }
}
