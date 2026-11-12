use std::{collections::BTreeSet, sync::LazyLock};
use tank::{AsValue, Entity, Executor, Result, Row, Sql, cols, expr, stream::TryStreamExt};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
#[tank(name = "case_orders")]
pub struct CaseOrder {
    #[tank(primary_key)]
    pub id: i32,
    pub status: i32,
    pub amount: f64,
}

pub async fn case(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    CaseOrder::drop_table(executor, true, false).await?;
    CaseOrder::create_table(executor, true, false).await?;
    CaseOrder::insert_many(
        executor,
        &[
            CaseOrder {
                id: 1,
                status: 1,
                amount: 50.0,
            },
            CaseOrder {
                id: 2,
                status: 2,
                amount: 150.0,
            },
            CaseOrder {
                id: 3,
                status: 3,
                amount: 20.0,
            },
            CaseOrder {
                id: 4,
                status: 9,
                amount: 500.0,
            },
        ],
    )
    .await?;

    // Find using simple condition
    let ids = CaseOrder::find_many(
        executor,
        expr!(
            (if CaseOrder::amount > 100.0 {
                "large"
            } else {
                "small"
            }) == "large"
        ),
        None,
    )
    .map_ok(|order| order.id)
    .try_collect::<BTreeSet<_>>()
    .await?;
    assert_eq!(ids, BTreeSet::from([2, 4]));

    // Simple CASE, with alternatives and a default
    let closed = 3;
    let ids = CaseOrder::find_many(
        executor,
        expr!(
            (match CaseOrder::status {
                1 => "open",
                2 | #closed => "closed",
                _ => "unknown",
            }) == "closed"
        ),
        None,
    )
    .map_ok(|order| order.id)
    .try_collect::<BTreeSet<_>>()
    .await?;
    assert_eq!(ids, BTreeSet::from([2, 3]));

    // CASE without ELSE is NULL when no branch matches
    let ids = CaseOrder::find_many(
        executor,
        expr!(
            (match CaseOrder::status {
                1 => "open",
            }) == NULL as IS
        ),
        None,
    )
    .map_ok(|order| order.id)
    .try_collect::<BTreeSet<_>>()
    .await?;
    assert_eq!(ids, BTreeSet::from([2, 3, 4]));

    // CASE as the whole condition, and inside a logical operator
    let ids = CaseOrder::find_many(
        executor,
        expr!(if CaseOrder::status == 1 {
            true
        } else {
            CaseOrder::amount > 400.0
        }),
        None,
    )
    .map_ok(|order| order.id)
    .try_collect::<BTreeSet<_>>()
    .await?;
    assert_eq!(ids, BTreeSet::from([1, 4]));
    let ids = CaseOrder::find_many(
        executor,
        expr!(
            CaseOrder::id > 1
                && (match CaseOrder::status {
                    2 => true,
                    _ => false,
                })
        ),
        None,
    )
    .map_ok(|order| order.id)
    .try_collect::<BTreeSet<_>>()
    .await?;
    assert_eq!(ids, BTreeSet::from([2]));

    // CASE in the projection
    let mut labels = executor
        .fetch(
            Sql.select(cols!(
                CaseOrder::id,
                if CaseOrder::amount >= 100.0 {
                    "large"
                } else {
                    "small"
                } as size
            ))
            .from(CaseOrder::table())
            .build(&executor.driver()),
        )
        .and_then(|row| async move {
            Ok((
                i32::try_from_value(row.value(0).unwrap())?,
                String::try_from_value(row.value(1).unwrap())?,
            ))
        })
        .try_collect::<Vec<_>>()
        .await?;
    labels.sort();
    assert_eq!(
        labels,
        [
            (1, "small".to_string()),
            (2, "large".to_string()),
            (3, "small".to_string()),
            (4, "large".to_string()),
        ]
    );
    Ok(())
}
