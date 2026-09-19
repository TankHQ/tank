use std::sync::LazyLock;
use tank::{Entity, Executor, expr, stream::TryStreamExt};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug)]
struct BinaryFields {
    #[tank(primary_key)]
    id: i32,
    data: Box<[u8]>,
}

/// A payload with every kind of byte, including invalid UTF-8 sequences.
fn payload() -> Box<[u8]> {
    vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x80, 0x7F, 0xC3].into_boxed_slice()
}

pub async fn binary(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;
    let payload = payload();

    // Setup
    BinaryFields::drop_table(executor, true, false)
        .await
        .expect("Failed to drop the BinaryFields table");
    BinaryFields::create_table(executor, true, true)
        .await
        .expect("Failed to create the BinaryFields table");

    // Literal insert must preserve every byte.
    BinaryFields::insert_one(
        executor,
        &BinaryFields {
            id: 1,
            data: payload.clone(),
        },
    )
    .await
    .expect("Failed to insert the binary payload");
    let loaded = BinaryFields::find_one(executor, expr!(BinaryFields::id == 1))
        .await
        .expect("Failed to query the binary payload")
        .expect("Failed to find the binary payload");
    assert_eq!(loaded.data, payload, "Binary literal round-trip lost bytes");

    // A bound parameter must preserve every byte too.
    let mut query = BinaryFields::prepare_find(executor, expr!(BinaryFields::data == ?), None)
        .await
        .expect("Failed to prepare the binary lookup");
    query
        .bind(payload.clone())
        .expect("Failed to bind the binary payload");
    let rows = executor
        .fetch(&mut query)
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to fetch the bound binary payload");
    assert_eq!(rows.len(), 1, "Expected exactly one match for the payload");
    let loaded = BinaryFields::from_row(rows.into_iter().next().unwrap())
        .expect("Failed to decode the bound binary payload");
    assert_eq!(loaded.data, payload, "Binary bound round-trip lost bytes");

    BinaryFields::drop_table(executor, true, false)
        .await
        .expect("Failed to drop the BinaryFields table");
}
