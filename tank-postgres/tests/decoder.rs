mod init;

#[cfg(test)]
mod tests {
    use super::init::init;
    use std::{borrow::Cow, sync::Mutex};
    use tank_core::{Connection, Executor, Row, Value};
    use tank_postgres::{PostgresConnection, PostgresDriver};

    static MUTEX: Mutex<()> = Mutex::new(());
    static DRIVER: PostgresDriver = PostgresDriver::new();

    /// Scalar decoders (int4/int8/text/bytea) must round-trip every input,
    /// including empty, non-ASCII and oversized payloads.
    #[tokio::test]
    async fn scalar_decoders_round_trip_representative_inputs() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let mut connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();

        for number in [i32::MIN, -1, 0, 1, i32::MAX] {
            let row = connection
                .fetch(format!("SELECT '{number}'::int4"))
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int32(Some(number)));
        }

        for text in ["", "abc", "too long", "Non-ASCII: \u{e9} \u{1f680}"] {
            let row = connection
                .fetch(format!("SELECT '{text}'::text"))
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(
                row.value(0).unwrap().as_ref(),
                &Value::Varchar(Some(text.into()))
            );
        }

        for bytes in ["\\x", "\\x0011ff", "\\xff"] {
            let row = connection
                .fetch(format!("SELECT '{bytes}'::bytea"))
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let Value::Blob(Some(raw)) = row.value(0).unwrap().into_owned() else {
                panic!("expected a blob");
            };
            let expected: Vec<u8> = bytes
                .trim_start_matches("\\x")
                .as_bytes()
                .chunks(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            assert_eq!(&raw[..], &expected[..]);
        }

        for number in [i64::MIN, -1, 0, 1, i64::MAX] {
            let row = connection
                .fetch(format!("SELECT '{number}'::int8"))
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(row.value(0).unwrap().as_ref(), &Value::Int64(Some(number)));
        }

        // A NULL for each scalar decodes to a null value.
        let row = connection
            .fetch("SELECT NULL::int4, NULL::text, NULL::bytea, NULL::int8")
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        for index in 0..4 {
            assert!(row.value(index).unwrap().is_null());
        }
    }

    /// Textual scalar types borrow from the driver buffer, so a retained row must
    /// own a copy that survives further queries.
    #[tokio::test]
    async fn borrowed_text_survives_buffer_reuse() {
        let _guard = MUTEX.lock().unwrap();
        let (url, _container) = init(false).await;
        let mut connection = PostgresConnection::connect(&DRIVER, url.into())
            .await
            .unwrap();
        let owned = {
            let row = connection
                .fetch("SELECT 'borrowed'::text")
                .await
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            row.into_owned()
        };
        connection
            .execute("SELECT 'overwrite the read buffer'")
            .await
            .unwrap();
        assert_eq!(
            owned.value(0).unwrap().as_ref(),
            &Value::Varchar(Some(Cow::Borrowed("borrowed")))
        );
    }
}
