use crate::silent_logs;
use serde_json::{Value as Json, json};
use tank::{Entity, Executor, Result, expr};

#[derive(Entity, Debug, PartialEq)]
struct JsonBlob {
    #[tank(primary_key)]
    id: i32,
    document: Json,
    bytes: Box<[u8]>,
}

pub async fn json_blob(executor: &mut impl Executor) -> Result<()> {
    silent_logs! {
        JsonBlob::drop_table(executor, true, false).await?;
    }
    JsonBlob::create_table(executor, true, true).await?;

    let json_blob = JsonBlob {
        id: 1,
        document: json!({
            "name": "tank",
            "nested": { "numbers": [1, 2, 3], "active": true },
        }),
        bytes: [0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x80, 0x7F].into(),
    };
    json_blob.save(executor).await?;
    let found = JsonBlob::find_one(executor, expr!(JsonBlob::id == 1)).await?;
    assert_eq!(
        found,
        Some(json_blob),
        "JSON or binary round-trip changed the payload"
    );
    Ok(())
}
