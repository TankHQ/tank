use std::sync::LazyLock;
use tank::{Entity, Executor, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Entity whose column names contain characters that require escaping
/// by the driver's identifier quoting mechanism (e.g. backticks in MySQL).
#[derive(Entity, Debug, PartialEq)]
#[tank(name = "special_ids")]
pub struct SpecialIdentifiers {
    #[tank(primary_key)]
    pub id: i32,
    #[tank(name = "back`tick")]
    pub backtick_col: String,
    #[tank(name = "double\"quote")]
    pub double_quote_col: i32,
}

pub async fn identifiers(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    SpecialIdentifiers::drop_table(executor, true, false)
        .await
        .expect("Failed to drop SpecialIdentifiers table");
    SpecialIdentifiers::create_table(executor, true, true)
        .await
        .expect("Failed to create SpecialIdentifiers table");

    let entity = SpecialIdentifiers {
        id: 1,
        backtick_col: "hello".to_string(),
        double_quote_col: 42,
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save entity with special identifier characters");

    let found = SpecialIdentifiers::find_one(executor, expr!(SpecialIdentifiers::id == 1))
        .await
        .expect("Failed to query")
        .expect("Entity not found");
    assert_eq!(found, entity);

    SpecialIdentifiers::delete_many(executor, expr!(SpecialIdentifiers::id == 1))
        .await
        .expect("Failed to delete");

    let gone = SpecialIdentifiers::find_one(executor, expr!(SpecialIdentifiers::id == 1))
        .await
        .expect("Failed to query after delete");
    assert!(gone.is_none());
}
