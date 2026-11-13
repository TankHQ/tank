use std::sync::LazyLock;
use tank::{
    AsValue, Entity, Executor, Result, Row, Sql, cols,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq, Eq)]
#[tank(schema = "testing")]
struct OptionRow {
    #[tank(primary_key)]
    seq: u32,
    bucket: u32,
}

#[derive(Entity, Debug, PartialEq, Eq)]
#[tank(schema = "testing")]
struct OptionTarget {
    #[tank(primary_key)]
    seq: u32,
    bucket: u32,
}

pub async fn select_options(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;
    let bucket_order = cols!(OptionRow::bucket ASC);
    let seq_order = cols!(OptionRow::seq ASC);

    OptionTarget::drop_table(executor, true, false).await?;
    OptionRow::drop_table(executor, true, false).await?;
    OptionRow::create_table(executor, false, true).await?;
    let rows = [
        OptionRow { seq: 1, bucket: 10 },
        OptionRow { seq: 2, bucket: 10 },
        OptionRow { seq: 3, bucket: 20 },
        OptionRow { seq: 4, bucket: 20 },
        OptionRow { seq: 5, bucket: 30 },
        OptionRow { seq: 6, bucket: 40 },
    ];
    OptionRow::insert_many(executor, &rows).await?;

    // SELECT DISTINCT bucket ORDER BY bucket
    let buckets = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::bucket))
                .from(OptionRow::table())
                .distinct()
                .order_by(bucket_order)
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(buckets, vec![10, 20, 30, 40]);

    // SELECT DISTINCT bucket ORDER BY bucket LIMIT 2 OFFSET 1
    let buckets = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::bucket))
                .from(OptionRow::table())
                .distinct()
                .order_by(bucket_order)
                .limit(Some(2))
                .offset(Some(1))
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(buckets, vec![20, 30]);

    // SELECT DISTINCT bucket ORDER BY bucket OFFSET 2
    let buckets = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::bucket))
                .from(OptionRow::table())
                .distinct()
                .order_by(bucket_order)
                .offset(Some(2))
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(buckets, vec![30, 40]);

    // SELECT seq ORDER BY seq LIMIT 2 OFFSET 2
    let seqs = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::seq))
                .from(OptionRow::table())
                .order_by(seq_order)
                .limit(Some(2))
                .offset(Some(2))
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(seqs, vec![3, 4]);

    // SELECT seq ORDER BY seq LIMIT 1 OFFSET 3
    let seqs = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::seq))
                .from(OptionRow::table())
                .order_by(seq_order)
                .limit(Some(1))
                .offset(Some(3))
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(seqs, vec![4]);

    // SELECT seq ORDER BY seq OFFSET 4
    let seqs = executor
        .fetch(
            Sql()
                .select(cols!(OptionRow::seq))
                .from(OptionRow::table())
                .order_by(seq_order)
                .offset(Some(4))
                .build(&executor.driver()),
        )
        .map(|row| u32::try_from_value(row?.value(0).unwrap()))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(seqs, vec![5, 6]);

    // INSERT INTO ... SELECT ... ORDER BY seq OFFSET 4
    #[cfg(not(feature = "disable-insert-select"))]
    {
        OptionTarget::create_table(executor, false, false).await?;
        executor
            .execute(
                Sql()
                    .insert_into(OptionTarget::table())
                    .select(OptionRow::columns())
                    .from(OptionRow::table())
                    .order_by(seq_order)
                    .offset(Some(4))
                    .build(&executor.driver()),
            )
            .await?;
        let mut copied = OptionTarget::find_many(executor, true, None)
            .try_collect::<Vec<_>>()
            .await?;
        copied.sort_by_key(|r| r.seq);
        assert_eq!(
            copied,
            vec![
                OptionTarget { seq: 5, bucket: 30 },
                OptionTarget { seq: 6, bucket: 40 },
            ]
        );
    }

    Ok(())
}
