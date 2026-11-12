#![allow(unused_imports)]
use std::sync::{Arc, LazyLock};
use tank::{AsValue, Entity, Executor, Result, Row, RowOwned, Sql, expr, stream::TryStreamExt};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(name = "insert_source")]
pub struct InsertSource {
    #[tank(primary_key)]
    pub id: i32,
    pub name: String,
    pub score: i32,
}

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(name = "insert_target")]
pub struct InsertTarget {
    #[tank(primary_key)]
    pub id: i32,
    pub name: String,
    pub score: i32,
}

async fn targets(executor: &mut impl Executor) -> Result<Vec<(i32, String, i32)>> {
    let mut rows = InsertTarget::find_many(executor, true, None)
        .map_ok(|t| (t.id, t.name, t.score))
        .try_collect::<Vec<_>>()
        .await?;
    rows.sort();
    Ok(rows)
}

pub async fn insert_builder(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    InsertSource::drop_table(executor, true, false).await?;
    InsertTarget::drop_table(executor, true, false).await?;
    InsertSource::create_table(executor, true, false).await?;
    InsertTarget::create_table(executor, true, false).await?;
    let sources = [
        InsertSource {
            id: 1,
            name: "one".into(),
            score: 5,
        },
        InsertSource {
            id: 2,
            name: "two".into(),
            score: 15,
        },
        InsertSource {
            id: 3,
            name: "three".into(),
            score: 25,
        },
    ];
    InsertSource::insert_many(executor, &sources).await?;

    // Entities into a table with the same shape
    executor
        .execute(
            Sql.insert_into(InsertTarget::table())
                .values(&sources[..2])
                .build(&executor.driver()),
        )
        .await?;
    assert_eq!(
        targets(executor).await?,
        [(1, "one".into(), 5), (2, "two".into(), 15)]
    );
    executor
        .execute(
            Sql.insert_into(InsertTarget::table())
                .values(&[InsertSource {
                    id: 2,
                    name: "TWO".into(),
                    score: 16,
                }])
                .update()
                .build(&executor.driver()),
        )
        .await?;
    assert_eq!(
        targets(executor).await?,
        [(1, "one".into(), 5), (2, "TWO".into(), 16)]
    );

    // Rows without an entity, upserting on the primary key of the target table
    {
        let labels: Arc<[String]> = ["id", "name", "score"].map(Into::into).into();
        let row = |id: i32, name: &str, score: i32| {
            RowOwned::new(
                labels.clone(),
                vec![id.as_value(), name.to_string().as_value(), score.as_value()],
            )
        };
        executor
            .execute(
                Sql.insert_into(InsertTarget::table())
                    .columns(labels.iter().cloned())
                    .values(
                        [row(3, "three", 25), row(4, "four", 35)]
                            .iter()
                            .map(Row::values),
                    )
                    .build(&executor.driver()),
            )
            .await?;
        executor
            .execute(
                Sql.insert_into(InsertTarget::table())
                    .columns(labels.iter().cloned())
                    .values([row(1, "ONE", 6)].iter().map(Row::values))
                    .update()
                    .build(&executor.driver()),
            )
            .await?;
        assert_eq!(
            targets(executor).await?,
            [
                (1, "ONE".into(), 6),
                (2, "TWO".into(), 16),
                (3, "three".into(), 25),
                (4, "four".into(), 35),
            ]
        );
    }

    // Rows returned by a query
    #[cfg(not(feature = "disable-insert-select"))]
    {
        InsertTarget::drop_table(executor, true, false).await?;
        InsertTarget::create_table(executor, true, false).await?;
        executor
            .execute(
                Sql.insert_into(InsertTarget::table())
                    .select(InsertSource::columns())
                    .from(InsertSource::table())
                    .where_expr(expr!(InsertSource::score > 10))
                    .build(&executor.driver()),
            )
            .await?;
        assert_eq!(
            targets(executor).await?,
            [(2, "two".into(), 15), (3, "three".into(), 25)]
        );
    }
    Ok(())
}
