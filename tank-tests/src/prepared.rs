#![allow(unused_imports)]
use std::pin::pin;
use tank::{
    Entity, Executor, Query, QueryBuilder, Result, Sql, cols, expr,
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
        .fetch(&mut query)
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
        .fetch(&mut query)
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
        .fetch(&mut query)
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

/// A query handed to `run` must survive the stream being dropped before it ends.
pub async fn prepared_abandoned_stream(executor: &mut impl Executor) -> Result<()> {
    PreparedBinding::drop_table(executor, true, false).await?;
    PreparedBinding::create_table(executor, true, false).await?;
    let entities = [
        PreparedBinding { id: 1, value: 2 },
        PreparedBinding {
            id: 4_000_000_000,
            value: -7,
        },
    ];
    PreparedBinding::insert_many(executor, entities.iter()).await?;

    let mut query = Sql()
        .select([PreparedBinding::id, PreparedBinding::value])
        .from(PreparedBinding::table())
        .order_by(cols!(PreparedBinding::id))
        .build(&executor.driver());
    let sql = format!("{query}");

    // Read a single row and drop the stream, as `take(1)`, a timeout or an early `?` would.
    {
        let mut stream = pin!(executor.run(&mut query));
        assert!(stream.next().await.is_some(), "Expected at least one row");
    }

    assert_eq!(
        format!("{query}"),
        sql,
        "An abandoned stream emptied the caller's query"
    );
    let loaded = executor
        .fetch(&mut query)
        .map_ok(PreparedBinding::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        loaded, entities,
        "The query did not run again after the stream was abandoned"
    );
    Ok(())
}

/// A query built by hand, prepared once and re-bound, must run as often as it is bound.
pub async fn prepared_builder_reuse(executor: &mut impl Executor) -> Result<()> {
    // Setup
    PreparedBinding::drop_table(executor, true, false).await?;
    PreparedBinding::create_table(executor, true, false).await?;
    let entities = [
        PreparedBinding { id: 1, value: 2 },
        PreparedBinding {
            id: 4_000_000_000,
            value: -7,
        },
    ];
    PreparedBinding::insert_many(executor, entities.iter()).await?;

    // Reuse prepared query
    let mut sql = Sql()
        .select([PreparedBinding::id, PreparedBinding::value])
        .from(PreparedBinding::table())
        .where_expr(expr!(PreparedBinding::id == ?))
        .build(&executor.driver());
    // Raw queries are borrowed by `prepare` and survive it; prepared payloads are moved out.
    let raw_query = matches!(sql, Query::Raw(..));
    let mut query = executor.prepare(&mut sql).await?;
    assert!(query.is_prepared(), "The built query was not prepared");
    assert!(!sql.is_prepared(), "The original changed");
    for entity in &entities {
        query.clear_bindings()?;
        query.bind(entity.id)?;
        let loaded = executor
            .fetch(&mut query)
            .map_ok(PreparedBinding::from_row)
            .map(Result::flatten)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            loaded,
            [entity.clone()],
            "The re-bound statement did not return the newly bound row"
        );
    }
    if raw_query {
        assert!(
            matches!(&sql, Query::Raw(raw) if raw.0.starts_with("SELECT")),
            "The built raw query was lost by prepare"
        );
    }
    Ok(())
}
