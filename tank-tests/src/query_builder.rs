use crate::{CREW, Crew, crew_queries};
use std::sync::LazyLock;
use tank::{BoxQueryBuilder, Entity, Executor, Result, stream::TryStreamExt};

use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn fetch_crew(
    executor: &mut impl Executor<Driver: 'static>,
    select: &BoxQueryBuilder,
) -> Result<Vec<Crew>> {
    let mut crew = executor
        .fetch(select)
        .and_then(|row| async { Crew::from_row(row) })
        .try_collect::<Vec<_>>()
        .await?;
    crew.sort_by_key(|member| member.id);
    Ok(crew)
}

/// Executes the driver agnostic [`crew_queries`] one at a time.
pub async fn query_builder(executor: &mut impl Executor<Driver: 'static>) -> Result<()> {
    let _lock = MUTEX.lock().await;

    let [
        drop_table,
        create_table,
        insert,
        select,
        update,
        insert_more,
        delete,
    ] = crew_queries();

    executor.execute(&drop_table).await?;
    executor.execute(&create_table).await?;

    let result = executor.execute(&insert).await?;
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 3);
    }
    assert_eq!(fetch_crew(executor, &select).await?, CREW[..3]);

    #[cfg(not(feature = "disable-updates"))]
    let expected = {
        let result = executor.execute(&update).await?;
        if let Some(affected) = result.rows_affected {
            assert_eq!(affected, 1);
        }
        let mut expected = CREW.to_vec();
        expected[1].rank = 10;
        assert_eq!(fetch_crew(executor, &select).await?, expected[..3]);
        expected
    };
    #[cfg(feature = "disable-updates")]
    let expected = {
        let _ = update;
        CREW.to_vec()
    };

    let result = executor.execute(&insert_more).await?;
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 2);
    }
    assert_eq!(fetch_crew(executor, &select).await?, expected);

    let result = executor.execute(&delete).await?;
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 5);
    }
    assert!(fetch_crew(executor, &select).await?.is_empty());
    Ok(())
}
