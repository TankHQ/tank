use crate::silent_logs;
use std::sync::LazyLock;
use tank::{Entity, Executor, Result, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
struct SavedRow {
    #[tank(primary_key)]
    id: i32,
    text: Option<String>,
    count: i32,
}

pub async fn save(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    silent_logs! {
        SavedRow::drop_table(executor, true, false).await?;
    }
    SavedRow::create_table(executor, false, true).await?;
    SavedRow {
        id: 1,
        text: Some("first".into()),
        count: 1,
    }
    .save(executor)
    .await?;

    // Saving again must replace the stored entity, including the fields that become NULL.
    let expected = SavedRow {
        id: 1,
        text: None,
        count: 2,
    };
    expected.save(executor).await?;
    assert_eq!(
        SavedRow::find_one(executor, expr!(SavedRow::id == 1)).await?,
        Some(expected),
        "Saving over an existing entity must replace it"
    );
    Ok(())
}
