use std::sync::LazyLock;
use tank::{
    AsValue, Entity, Executor, QueryBuilder, Result, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Default, Entity, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct MathTable {
    #[tank(primary_key)]
    id: u64,
    read: u64,
}

pub async fn math<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    MathTable::drop_table(executor, true, false)
        .await
        .expect("Failed to drop MathTable table");
    MathTable::create_table(executor, false, false)
        .await
        .expect("Failed to create MathTable table");
    MathTable { id: 0, read: 0 }
        .save(executor)
        .await
        .expect("Could not save the dummy entry");

    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MathTable::id, ((42 * 6 + 56) / 7) as read))
                .from(MathTable::table())
                .where_expr(expr!(MathTable::id == 0))
                .build(&executor.driver()),
        )
        .map_ok(MathTable::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the result 1");
    assert_eq!(result, [MathTable { id: 0, read: 44 }]);

    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MathTable::id, ((5 - (1 << 2)) * 9 % 6) as read))
                .from(MathTable::table())
                .where_expr(expr!(MathTable::id == 0))
                .build(&executor.driver()),
        )
        .map_ok(MathTable::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the result 2");
    assert_eq!(result, [MathTable { id: 0, read: 3 }]);

    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MathTable::id, ((1 | 2 | 4) + 1) as read))
                .from(MathTable::table())
                .where_expr(expr!(MathTable::id == 0))
                .build(&executor.driver()),
        )
        .map_ok(MathTable::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the result 3");
    assert_eq!(result, [MathTable { id: 0, read: 8 }]);

    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MathTable::id, (90 > 89 && (10 & 6) == 2) as read))
                .from(MathTable::table())
                .where_expr(expr!(MathTable::id == 0))
                .build(&executor.driver()),
        )
        .map_ok(|v| bool::try_from_value(v.values.into_iter().nth(1).unwrap()))
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the result 4");
    assert_eq!(result, [true]);

    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(MathTable::id, (4 == (2, 3, 4, 5) as IN) as read))
                .from(MathTable::table())
                .where_expr(expr!(MathTable::id == 0))
                .build(&executor.driver()),
        )
        .map_ok(|v| bool::try_from_value(v.values.into_iter().nth(1).unwrap()))
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not get the result 5");
    assert_eq!(result, [true]);
}
