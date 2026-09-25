#![allow(unused_imports)]
use tank::{Entity, Executor, Result, expr, stream::TryStreamExt};

#[derive(Entity, Debug, PartialEq)]
#[tank(schema = "testing", name = "prepared_binding")]
pub struct PreparedBinding {
    #[tank(primary_key)]
    pub id: u64,
    pub value: i64,
}

pub async fn prepared(executor: &mut impl Executor) {
    PreparedBinding::drop_table(executor, true, false)
        .await
        .expect("Failed to drop the PreparedBinding table");
    PreparedBinding::create_table(executor, true, false)
        .await
        .expect("Failed to create the PreparedBinding table");

    let entity = PreparedBinding {
        id: 4_000_000_000,
        value: -7,
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save the PreparedBinding entity");

    let mut query =
        PreparedBinding::prepare_find(executor, expr!(PreparedBinding::id == ?), Some(1))
            .await
            .expect("Failed to prepare the bound-key query");
    query
        .bind(entity.id)
        .expect("Failed to bind the key parameter");
    let loaded = executor
        .fetch(query)
        .and_then(|row| async move { PreparedBinding::from_row(row) })
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query by the bound key");
    assert_eq!(loaded, [entity], "Bound key parameter did not round-trip");
}
