mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::{borrow::Cow, pin::pin, sync::Mutex};
    use tank_core::{
        Connection, Executor, Row, RowOwned, Value,
        stream::{StreamExt, TryStreamExt},
    };
    use tank_postgres::{PostgresConnection, PostgresDriver};

    static MUTEX: Mutex<()> = Mutex::new(());
    static DRIVER: PostgresDriver = PostgresDriver::new();

    async fn connect() -> (PostgresConnection, impl std::fmt::Debug) {
        let (url, container) = init(false).await;
        let connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();
        (connection, container)
    }

    /// Fetches exactly one row and owns it, releasing the connection borrow.
    async fn fetch_one(
        connection: &mut PostgresConnection,
        query: impl tank_core::AsQuery<PostgresDriver> + Send,
    ) -> RowOwned {
        let mut rows = pin!(connection.fetch(query));
        rows.next().await.unwrap().unwrap().into_owned()
    }

    #[tokio::test]
    async fn reuses_parameter_storage_and_resets_bindings() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let mut query = connection.prepare("SELECT $1::text").await.unwrap();

        for (value, expected) in [
            ("a reasonably long string", "a reasonably long string"),
            ("shorter", "shorter"),
            ("", ""),
        ] {
            query.bind(value).unwrap();
            let row = fetch_one(&mut connection, &mut query).await;
            assert_eq!(
                row.value(0).unwrap().as_ref(),
                &Value::Varchar(Some(expected.into()))
            );
        }

        // Binding NULL reuses the same statement with a null parameter.
        query.bind(None::<String>).unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert!(row.value(0).unwrap().is_null());

        // A further rebind after a NULL still produces the right value.
        query.bind("after null").unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(
            row.value(0).unwrap().as_ref(),
            &Value::Varchar(Some("after null".into()))
        );
    }

    #[tokio::test]
    async fn indexed_binding_replaces_without_shifting_other_parameters() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let mut query = connection.prepare("SELECT $1::int, $2::int").await.unwrap();
        query.bind(1_i32).unwrap().bind(2_i32).unwrap();
        query.bind_index(3_i32, 0).unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int32(Some(3)));
        assert_eq!(row.value(1).unwrap().as_ref(), &Value::Int32(Some(2)));
    }

    #[tokio::test]
    async fn owns_borrowed_bindings() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let mut query = connection
            .prepare("SELECT $1::text, $2::bytea")
            .await
            .unwrap();
        {
            let text = String::from("borrowed text");
            let bytes = vec![0_u8, 17, 255];
            query.bind(text.as_str()).unwrap();
            query
                .bind(Value::Blob(Some(Cow::Borrowed(&bytes))))
                .unwrap();
        }
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(
            row.value(0).unwrap().as_ref(),
            &Value::Varchar(Some("borrowed text".into()))
        );
        assert_eq!(
            row.value(1).unwrap().as_ref(),
            &Value::Blob(Some(Cow::Borrowed(&[0, 17, 255])))
        );
    }

    #[tokio::test]
    async fn clears_bindings_after_partial_encoding_failure() {
        let _guard = MUTEX.lock().unwrap();
        let (mut connection, _container) = connect().await;
        let mut query = connection.prepare("SELECT $1::int, $2::int").await.unwrap();
        query.bind(1_i32).unwrap().bind(2_i32).unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int32(Some(1)));
        assert_eq!(row.value(1).unwrap().as_ref(), &Value::Int32(Some(2)));

        // A failed local encode must not leave stale bindings behind.
        query.bind(3_i32).unwrap();
        query
            .bind(Value::Unknown(Some("unsupported".into())))
            .unwrap();
        assert!(
            connection
                .fetch(&mut query)
                .map_ok(Row::into_owned)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );

        query.bind(4_i32).unwrap().bind(5_i32).unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int32(Some(4)));
        assert_eq!(row.value(1).unwrap().as_ref(), &Value::Int32(Some(5)));

        query.bind(6_i32).unwrap();
        query.clear_bindings().unwrap();
        query.bind(7_i32).unwrap().bind(8_i32).unwrap();
        let row = fetch_one(&mut connection, &mut query).await;
        assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int32(Some(7)));
        assert_eq!(row.value(1).unwrap().as_ref(), &Value::Int32(Some(8)));
    }
}
