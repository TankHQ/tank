use std::{pin::pin, sync::LazyLock};
use tank::{
    Connection, Entity, Executor, QueryBuilder, Transaction, cols, future::join, stream::StreamExt,
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity)]
struct Table {
    #[tank(primary_key)]
    id: u32,
    value: i32,
}

pub async fn transaction3(connection: &mut impl Connection) {
    let _lock = MUTEX.lock().await;

    // Setup
    Table::drop_table(connection, true, false)
        .await
        .expect("Failed to drop Table table");
    Table::create_table(connection, true, true)
        .await
        .expect("Failed to create Table table");

    Table::insert_many(
        connection,
        &[
            Table { id: 1, value: 5 },
            Table { id: 2, value: 9 },
            Table { id: 3, value: -2 },
        ],
    )
    .await
    .expect("Failed to insert values");
    async fn query_sum(c: &mut impl Executor) -> Table {
        Table::from_row(
            pin!(
                c.fetch(
                    QueryBuilder::new()
                        .select(cols!(0 as id, SUM(value) as value))
                        .from(Table::table())
                        .build(&c.driver()),
                )
            )
            .next()
            .await
            .expect("Didn't find the result")
            .expect("Error while fetching"),
        )
        .expect("Unexpected row format")
    }
    let sum = query_sum(connection).await;
    assert_eq!(sum.id, 0);
    assert_eq!(sum.value, 12);

    let mut c1 = connection.duplicate().await.expect("Failed to duplicate 1");
    let mut c2 = connection.duplicate().await.expect("Failed to duplicate 2");
    let mut t1 = c1.begin().await.expect("Failed to begin 1");
    let mut t2 = c2.begin().await.expect("Failed to begin 2");
    let f1 = async {
        Table { id: 4, value: 4 }
            .save(&mut t1)
            .await
            .expect("Failed to save 1");
        assert_eq!(query_sum(&mut t1).await.value, 16);
        t1.commit().await.expect("Failed to commit t1");
    };
    let f2 = async {
        Table { id: 5, value: 3 }
            .save(&mut t2)
            .await
            .expect("Failed to save 2");
        let sum = query_sum(&mut t2).await.value;
        assert!(matches!(sum, 15 | 19));
        t2.rollback().await.expect("Failed to rollback");
    };
    join(f1, f2).await;
}
