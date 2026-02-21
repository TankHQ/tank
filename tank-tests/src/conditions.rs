use std::sync::LazyLock;
use tank::{
    Entity, Executor, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
#[tank(schema = "testing", name = "conditions")]
struct ConditionEntry {
    #[tank(primary_key)]
    id: i32,
    name: Option<String>,
    active: bool,
}

pub async fn conditions<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    ConditionEntry::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ConditionEntry table");
    ConditionEntry::create_table(executor, true, true)
        .await
        .expect("Failed to create ConditionEntry table");

    // Operations
    let entries = vec![
        ConditionEntry {
            id: 1,
            name: Some("Alice".into()),
            active: true,
        },
        ConditionEntry {
            id: 2,
            name: Some("Bob".into()),
            active: false,
        },
        ConditionEntry {
            id: 3,
            name: None,
            active: true,
        },
        ConditionEntry {
            id: 4,
            name: Some("Charlie".into()),
            active: true,
        },
    ];
    ConditionEntry::insert_many(executor, &entries)
        .await
        .expect("Failed to insert entries");

    let count = ConditionEntry::find_many(executor, expr!(name != NULL), None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(count, 3, "Should find 3 entries where `name IS NOT NULL`");

    let count = ConditionEntry::find_many(executor, expr!(name == NULL), None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(count, 1, "Should find 1 entry where `name IS NULL`");

    let count = ConditionEntry::find_many(executor, expr!(id == (1, 3, 5) as IN), None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(count, 2, "Should find 2 entries with `id IN (1, 3, 5)`");

    let count = ConditionEntry::find_many(executor, expr!(!active), None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(count, 1, "Should find 1 inactive entry");

    let count = ConditionEntry::find_many(executor, expr!(id > 3 && active == true), None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(count, 1, "Should find 1 entry with `id > 3` and active");

    let count =
        ConditionEntry::find_many(executor, expr!(ConditionEntry::name == "%e" as LIKE), None)
            .map_err(|e| panic!("{e:#}"))
            .count()
            .await;
    assert_eq!(count, 2, "Should find 2 entry with `name LIKE '%e'`");
}
