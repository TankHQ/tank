use crate::silent_logs;
use std::sync::LazyLock;
use tank::{AsValue, Entity, Executor, Result, Row, Sql, cols, expr, stream::TryStreamExt};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
struct ProjectionRow {
    #[tank(primary_key)]
    id: i32,
    value: i32,
    label: String,
}

pub async fn projections(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;
    silent_logs! {
        ProjectionRow::drop_table(executor, true, false).await?;
    }
    ProjectionRow::create_table(executor, false, true).await?;
    let expected = ProjectionRow {
        id: 1,
        value: 7,
        label: "seven".into(),
    };
    expected.save(executor).await?;

    // A projection of exactly one column must still return the row.
    {
        let rows = executor
            .fetch(
                Sql()
                    .select(cols!(ProjectionRow::value))
                    .from(ProjectionRow::table())
                    .where_expr(expr!(ProjectionRow::id == 1))
                    .build(&executor.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(rows.len(), 1, "A single-column projection returned no rows");
        assert_eq!(i32::decode_value(rows[0].value(0).unwrap())?, 7);
    }

    // `SELECT *` over an entity table must return every column.
    {
        let rows = executor
            .fetch(
                Sql()
                    .select(cols!(*))
                    .from(ProjectionRow::table())
                    .where_expr(expr!(ProjectionRow::id == 1))
                    .build(&executor.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(rows.len(), 1, "SELECT * returned no rows");
        assert_eq!(
            ProjectionRow::from_row(rows.into_iter().next().unwrap())?,
            expected
        );
    }
    Ok(())
}
