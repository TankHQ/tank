use crate::silent_logs;
use std::collections::BTreeSet;
use std::{pin::pin, sync::LazyLock};
use tank::{AsValue, Entity, Result, Row, expr, stream::StreamExt, stream::TryStreamExt};
use tank::{Executor, Sql, cols};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const EXPECTED_SUM: u32 = 68978385; // 1 + .. + 11745 = 68978385
const EXPECTED_AVG: u32 = 5873; // avg(1, .., 11745) = 5873
const COUNT: u64 = 11745;

#[derive(Default, Entity)]
struct Values {
    id: u64,
    value: u32,
}

pub async fn aggregates(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock();

    // Setup
    silent_logs! {
        Values::drop_table(executor, true, false).await?;
    }
    Values::create_table(executor, false, false).await?;

    // Insert 1..11746
    let values = (1..11746).map(|value| Values {
        id: value,
        value: value as u32,
    });
    let result = Values::insert_many(executor, values).await?;
    if let Some(affected) = result.rows_affected {
        assert_eq!(
            affected, COUNT,
            "Values::insert_many should have affected {COUNT} rows",
        );
    }

    // SELECT COUNT(*), SUM(value)
    {
        let mut stream = pin!(
            executor.fetch(
                Sql()
                    .select(cols!(COUNT(*), SUM(Values::value)))
                    .from(Values::table())
                    .build(&executor.driver())
            )
        );
        let row = stream
            .try_next()
            .await?
            .expect("The query is expected to return a row");
        assert!(
            stream.try_next().await?.is_none(),
            "The query is expected to return a single row"
        );
        let expected = (COUNT as _, EXPECTED_SUM as _);
        let actual = (
            i128::decode_value(row.value(0).unwrap())?,
            i128::decode_value(row.value(1).unwrap())?,
        );
        assert_eq!(
            actual, expected,
            "SELECT COUNT(*), SUM(value) is expected to return {expected:?} but it produced {actual:?}",
        );
    }

    // SELECT *
    {
        let values = executor
            .fetch(
                Sql()
                    .select(cols!(*))
                    .from(Values::table())
                    .where_expr(true)
                    .build(&executor.driver()),
            )
            .map(|row| {
                u32::decode_value(
                    row?.get_column("value")
                        .expect("Column `value` is not present"),
                )
            })
            .try_collect::<BTreeSet<_>>()
            .await?;
        assert!(
            values.into_iter().eq(1..=(COUNT as u32)),
            "The result didn't received from the db contains all the values that were inserted"
        );
    }

    // SELECT value WHERE value > ?
    {
        let mut query = Values::prepare_find(executor, expr!(Values::value > ?), None).await?;
        assert!(query.is_prepared());
        query.bind(EXPECTED_AVG)?;
        let len = executor
            .fetch(&mut query)
            .try_collect::<Vec<_>>()
            .await?
            .len();
        assert!(query.is_prepared());
        assert_eq!(len, COUNT as usize / 2);
        query.clear_bindings()?;
        query.bind(0)?;
        let values = executor.fetch(&mut query).try_collect::<Vec<_>>().await?;
        assert_eq!(values.len(), COUNT as usize);
    }
    Ok(())
}
