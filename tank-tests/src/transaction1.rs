use std::sync::LazyLock;
use tank::{Connection, DataSet, Entity, Executor, Transaction, cols, stream::TryStreamExt};
use tokio::sync::Mutex;

#[derive(Entity)]
struct EntityA {
    #[tank(primary_key)]
    name: String,
    field: i64,
}
#[derive(Entity)]
struct EntityB {
    #[tank(primary_key)]
    name: String,
    field: i64,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn transaction1<C: Connection>(connection: &mut C) {
    let _lock = MUTEX.lock().await;

    let mut transaction = connection
        .begin()
        .await
        .expect("Could not begin a transaction");

    // Setup
    EntityA::drop_table(&mut transaction, true, false)
        .await
        .expect("Failed to drop EntityA table");
    EntityA::create_table(&mut transaction, true, true)
        .await
        .expect("Failed to create EntityA table");
    EntityB::drop_table(&mut transaction, true, false)
        .await
        .expect("Failed to drop EntityB table");
    EntityB::create_table(&mut transaction, true, true)
        .await
        .expect("Failed to create EntityB table");
    transaction
        .commit()
        .await
        .expect("Filed to commit the transaction");

    let mut transaction = connection
        .begin()
        .await
        .expect("Could not begin a transaction");
    EntityA::insert_many(
        &mut transaction,
        &[
            EntityA {
                name: "first entity".into(),
                field: 5832,
            },
            EntityA {
                name: "second entity".into(),
                field: 48826,
            },
            EntityA {
                name: "third entity".into(),
                field: 48826,
            },
            EntityA {
                name: "fourth entity".into(),
                field: 48826,
            },
            EntityA {
                name: "fifth entity".into(),
                field: 48826,
            },
            EntityA {
                name: "sixth entity".into(),
                field: 48826,
            },
        ],
    )
    .await
    .expect("Failed to insert 6 EntityA");
    let entities = EntityA::table()
        .select(&mut transaction, cols!(*), &true, None)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not select EntityA rows");
    assert_eq!(entities.len(), 6);

    EntityB::insert_one(
        &mut transaction,
        &EntityB {
            name: "EntityB".into(),
            field: 5883,
        },
    )
    .await
    .expect("Failed to save EntityB");
    transaction
        .rollback()
        .await
        .expect("Failed to rollback the transaction");

    let mut transaction = connection
        .begin()
        .await
        .expect("Could not begin a transaction");
    let entities = EntityA::table()
        .select(&mut transaction, cols!(*), &true, None)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not select EntityA rows");
    assert_eq!(entities.len(), 0);
}
