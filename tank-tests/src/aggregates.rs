use std::collections::BTreeSet;
use std::{pin::pin, sync::LazyLock};
use tank::{
    AsValue, DataSet, Entity, Passive, RowLabeled, expr, stream::StreamExt, stream::TryStreamExt,
};
use tank::{Executor, cols};
use tokio::sync::Mutex;

#[derive(Default, Entity)]
struct Values {
    id: Passive<u64>,
    /// This column contains the actual value
    value: u32,
}
const EXPECTED_SUM: u32 = 68978385;
const EXPECTED_AVG: u32 = 5873;
const COUNT: u32 = 11745;
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn aggregates<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock();

    // Setup
    Values::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Values table");
    Values::create_table(executor, false, false)
        .await
        .expect("Failed to create Values table");

    // Insert
    // 1 + .. + 11745 = 68978385
    // avg(1, .., 11745) = 5873
    let mut values = (1..11746).map(|value| Values {
        id: value.into(),
        value: value as u32,
    });
    loop {
        let rows = values.by_ref().take(2000).collect::<Vec<_>>();
        if rows.is_empty() {
            break;
        }
        let result = Values::insert_many(executor, rows.iter()).await;
        assert!(
            result.is_ok(),
            "Failed to Values::insert_many: {:?}",
            result.unwrap_err()
        );
        let result = result.unwrap();
        if let Some(affected) = result.rows_affected {
            assert_eq!(
                affected,
                rows.len() as u64,
                "Values::insert_many should have affected {} rows",
                rows.len()
            );
        }
    }

    // SELECT COUNT(*), SUM(value)
    {
        let mut stream = pin!(Values::table().select(
            executor,
            cols!(COUNT(*), SUM(Values::value)),
            &true,
            None
        ));
        let count = stream.next().await;
        assert!(
            stream.next().await.is_none(),
            "The query is expected to return a single row"
        );
        let expected = (COUNT as i128, EXPECTED_SUM as i128);
        let actual = match count {
            Some(Ok(RowLabeled { values, .. })) => {
                let a = i128::try_from_value((*values)[0].clone());
                let b = i128::try_from_value((*values)[1].clone());
                match (a, b) {
                    (Ok(a), Ok(b)) => Some((a, b)),
                    (Err(e), _) => panic!("{e}"),
                    (_, Err(e)) => panic!("{e}"),
                }
            }
            _ => None,
        };
        assert_eq!(
            actual,
            Some(expected),
            "SELECT COUNT(*), SUM(value) is expected to return {:?}",
            expected
        );
    }

    // SELECT *
    {
        {
            let stream = pin!(Values::table().select(executor, cols!(*), &true, None));
            let values = stream
                .map(|row| {
                    let row = row.expect("Error while fetching the row");
                    let i = row
                        .names()
                        .iter()
                        .enumerate()
                        .find_map(|(i, v)| if v == "value" { Some(i) } else { None })
                        .expect("Column `value` is not present");
                    u32::try_from_value(row.values[i].clone())
                        .expect("The result could not be convert back to u32")
                })
                .collect::<BTreeSet<_>>()
                .await;
            assert!(
                values.into_iter().eq((1..(COUNT + 1)).into_iter()),
                "The result didn't received from the db contains all the values that were inserted"
            );
        }
    }

    // SELECT value WHERE value > ?
    {
        let mut query = Values::prepare_find(executor, &expr!(Values::value > ?), None)
            .await
            .expect("Failed to prepare the query");
        assert!(query.is_prepared());
        query
            .bind(EXPECTED_AVG)
            .expect("Could not bind the parameter");
        let values = executor
            .fetch(&mut query)
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not fetch rows above average from the prepared statement");
        assert!(query.is_prepared());
        assert_eq!(values.len(), COUNT as usize / 2);
        query
            .clear_bindings()
            .expect("Could not clear the bindings");
        query.bind(0).expect("Could not bind a second time");
        let values = executor
            .fetch(&mut query)
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not fetch positive rows from the prepared statement");
        assert_eq!(values.len(), COUNT as usize);
    }
}
