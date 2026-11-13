#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use std::{pin::pin, str::FromStr, sync::LazyLock};
use tank::{
    Driver, DynQuery, Entity, Executor, Interval, Query, QueryResult, RawQuery, Result, Sql,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
struct Container {
    first: [[Interval; 2]; 1],
    second: [[[Uuid; 1]; 3]; 1],
}

pub async fn arrays2(executor: &mut impl Executor) -> Result<()> {
    let _ = MUTEX.lock().await;
    #[cfg(feature = "disable-multiple-statements")]
    let _ = executor;

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = DynQuery::default();
        let writer = executor.driver().sql_writer();
        writer.write_drop_table(&mut query, Container::table(), true);
        writer.write_create_table(&mut query, Container::table(), true);
        let value = Container {
            first: [[
                Interval::from_years(500),
                Interval::from_micros(1) + Interval::from_months(4),
            ]],
            second: [[
                [Uuid::from_str("c1412337-cfce-444a-ac46-5e6edcc0bf23").unwrap()],
                [Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap()],
                [Uuid::from_str("9d7f0f5b-19d6-4298-a332-214fc85e2652").unwrap()],
            ]],
        };
        Sql()
            .insert_into(Container::table())
            .values([value].iter())
            .build_into(&driver, &mut query);
        Sql()
            .select(Container::columns())
            .from(Container::table())
            .where_expr(true)
            .limit(Some(1))
            .build_into(&driver, &mut query);
        let rows = executor
            .run(query)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|v| match v {
                QueryResult::Row(v) => Some(v),
                QueryResult::Affected(..) => None,
            })
            .map(Container::from_row)
            .collect::<tank::Result<Vec<_>>>()?;
        assert_eq!(
            rows,
            [Container {
                first: [[
                    Interval::from_years(500),
                    Interval::from_micros(1) + Interval::from_months(4),
                ]],
                second: [[
                    [Uuid::from_str("c1412337-cfce-444a-ac46-5e6edcc0bf23").unwrap()],
                    [Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap()],
                    [Uuid::from_str("9d7f0f5b-19d6-4298-a332-214fc85e2652").unwrap()],
                ]],
            }]
        )
    }
    Ok(())
}
