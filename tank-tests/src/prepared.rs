#![allow(unused_imports)]
use tank::{
    Entity, Executor, Result, expr,
    stream::{StreamExt, TryStreamExt},
};

#[derive(Entity, Debug, PartialEq, Clone)]
#[tank(schema = "testing", name = "prepared_binding")]
pub struct PreparedBinding {
    #[tank(primary_key)]
    pub id: u64,
    pub value: i64,
}

pub async fn prepared(executor: &mut impl Executor) -> Result<()> {
    // Setup
    PreparedBinding::drop_table(executor, true, false).await?;
    PreparedBinding::create_table(executor, true, false).await?;

    // Insert PreparedBinding then fire a prepared query
    let entity = PreparedBinding {
        id: 4_000_000_000,
        value: -7,
    };
    entity.save(executor).await?;

    let mut query =
        PreparedBinding::prepare_find(executor, expr!(PreparedBinding::id == ?), Some(1)).await?;
    query.bind(4_000_000_000_u64)?;
    let loaded = executor
        .fetch(query)
        .map_ok(PreparedBinding::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded,
        [entity.clone()],
        "Bound key parameter did not round-trip"
    );

    // A value above i32::MAX must be widened by the driver, not truncated to i32.
    query.clear_bindings()?;
    query.bind(4_000_000_000_u32)?;
    let loaded = executor
        .fetch(query)
        .map_ok(PreparedBinding::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded,
        [entity.clone()],
        "Bound u32 parameter above i32::MAX did not round-trip"
    );

    let mut query =
        PreparedBinding::prepare_find(executor, expr!(PreparedBinding::id == ?), Some(1)).await?;
    query.bind_index(entity.id, 0)?;
    let loaded = executor
        .fetch(query)
        .map_ok(PreparedBinding::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded,
        [entity.clone()],
        "bind_index(0) did not bind the first parameter"
    );

    // The same prepared statement must be reusable with a different binding.
    let mut query =
        PreparedBinding::prepare_find(executor, expr!(PreparedBinding::id == ?), Some(1)).await?;
    let mut found: Vec<PreparedBinding> = Vec::new();
    for expected in [entity.clone(), PreparedBinding { id: 1, value: 2 }] {
        if expected.id == 1 {
            expected.save(executor).await?;
        }
        query.bind(expected.id)?;
        let loaded = executor
            .fetch(&mut query)
            .map_ok(PreparedBinding::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await?;
        found.extend(loaded);
    }
    found.sort_by_key(|v| v.id);
    assert_eq!(
        found,
        [
            PreparedBinding { id: 1, value: 2 },
            PreparedBinding {
                id: 4_000_000_000,
                value: -7,
            },
        ],
        "Reusing a prepared query did not re-read the bindings"
    );
    Ok(())
}
