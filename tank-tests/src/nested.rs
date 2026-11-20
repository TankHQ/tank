use crate::silent_logs;
#[cfg(not(feature = "disable-maps"))]
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tank::{Entity, Executor, Result, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug)]
struct NestedBytes {
    #[tank(primary_key)]
    id: i32,
    #[cfg(not(feature = "disable-lists"))]
    blobs: Vec<Box<[u8]>>,
    #[cfg(not(feature = "disable-maps"))]
    named: BTreeMap<String, Box<[u8]>>,
}

#[cfg(not(feature = "disable-lists"))]
#[derive(Entity, PartialEq, Debug)]
struct SavedList {
    #[tank(primary_key)]
    id: i32,
    tags: Vec<String>,
}

#[cfg(not(feature = "disable-maps"))]
#[derive(Entity, PartialEq, Debug)]
struct SavedMap {
    #[tank(primary_key)]
    id: i32,
    attributes: BTreeMap<String, String>,
}

pub async fn nested(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    silent_logs! {
        NestedBytes::drop_table(executor, true, false).await?;
        #[cfg(not(feature = "disable-lists"))]
        SavedList::drop_table(executor, true, false).await?;
        #[cfg(not(feature = "disable-maps"))]
        SavedMap::drop_table(executor, true, false).await?;
    }
    NestedBytes::create_table(executor, false, true).await?;
    #[cfg(not(feature = "disable-lists"))]
    SavedList::create_table(executor, false, true).await?;
    #[cfg(not(feature = "disable-maps"))]
    SavedMap::create_table(executor, false, true).await?;

    // Binary payloads must keep every byte inside lists and maps.
    let bytes = NestedBytes {
        id: 1,
        #[cfg(not(feature = "disable-lists"))]
        blobs: vec![
            vec![0x00, 0xFF, 0xDE, 0xAD].into_boxed_slice(),
            vec![0x80, 0x7F, 0xC3].into_boxed_slice(),
        ],
        #[cfg(not(feature = "disable-maps"))]
        named: BTreeMap::from([("raw".to_string(), vec![0x00, 0xFE].into_boxed_slice())]),
    };
    NestedBytes::insert_one(executor, &bytes).await?;
    assert_eq!(
        NestedBytes::find_one(executor, expr!(NestedBytes::id == 1)).await?,
        Some(bytes),
        "Nested binary round-trip lost bytes"
    );

    // Saving over a list must drop the elements that disappeared.
    #[cfg(not(feature = "disable-lists"))]
    {
        SavedList {
            id: 1,
            tags: vec!["a".to_string(), "b".to_string()],
        }
        .save(executor)
        .await?;
        let list = SavedList {
            id: 1,
            tags: vec!["c".to_string()],
        };
        list.save(executor).await?;
        assert_eq!(
            SavedList::find_one(executor, expr!(SavedList::id == 1)).await?,
            Some(list),
            "Saving must drop the list elements that disappeared"
        );
    }

    // Saving over a map must drop the entries that disappeared.
    #[cfg(not(feature = "disable-maps"))]
    {
        SavedMap {
            id: 1,
            attributes: BTreeMap::from([
                ("one".to_string(), "1".to_string()),
                ("two".to_string(), "2".to_string()),
            ]),
        }
        .save(executor)
        .await?;
        let map = SavedMap {
            id: 1,
            attributes: BTreeMap::from([("two".to_string(), "updated".to_string())]),
        };
        map.save(executor).await?;
        assert_eq!(
            SavedMap::find_one(executor, expr!(SavedMap::id == 1)).await?,
            Some(map),
            "Saving must drop the map entries that disappeared"
        );
    }
    Ok(())
}
