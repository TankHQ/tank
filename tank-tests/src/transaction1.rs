use std::sync::LazyLock;
use tank::{
    Connection, Entity, QueryBuilder, Result, Transaction, cols,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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

/// Test the transaction functionality using only inserts and deletes (no select)
pub async fn transaction1(connection: &mut impl Connection) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    EntityA::drop_table(connection, true, false).await?;
    EntityA::create_table(connection, true, true).await?;
    EntityB::drop_table(connection, true, false).await?;
    EntityB::create_table(connection, true, true).await?;

    // Insert values and rollback
    let mut transaction = connection.begin().await?;

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
    .await?;

    EntityB::insert_one(
        &mut transaction,
        &EntityB {
            name: "EntityB".into(),
            field: 5883,
        },
    )
    .await?;

    transaction.rollback().await?;

    // Expect empty tables
    {
        let entities = connection
            .fetch(
                QueryBuilder::new()
                    .select(cols!(*))
                    .from(EntityA::table())
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
        assert!(entities.is_empty());
    }

    {
        let entities = connection
            .fetch(
                QueryBuilder::new()
                    .select(cols!(*))
                    .from(EntityB::table())
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
        assert!(entities.is_empty());
    }

    // Insert more values and commit
    let mut transaction = connection.begin().await?;

    EntityA {
        name: "myField".into(),
        field: 777,
    }
    .save(&mut transaction)
    .await?;

    EntityA {
        name: "mySecondField".into(),
        field: 999,
    }
    .save(&mut transaction)
    .await?;

    EntityB::insert_many(
        &mut transaction,
        &[
            EntityB {
                name: "aa".into(),
                field: 11,
            },
            EntityB {
                name: "bb".into(),
                field: 22,
            },
            EntityB {
                name: "cc".into(),
                field: 33,
            },
            EntityB {
                name: "dd".into(),
                field: 44,
            },
        ],
    )
    .await?;

    EntityB {
        name: "aa".into(),
        field: 11,
    }
    .delete(&mut transaction)
    .await?;

    transaction.commit().await?;

    let entity_a_entries = EntityA::find_many(connection, true, None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(entity_a_entries, 2);

    let entity_b_entries = EntityB::find_many(connection, true, None)
        .map_err(|e| panic!("{e:#}"))
        .count()
        .await;
    assert_eq!(entity_b_entries, 3);
    Ok(())
}
