#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    pin::pin,
    str::FromStr,
    sync::LazyLock,
};
use tank::{
    AnyQuery, Driver, Entity, Executor, Interval, Query, QueryResult, RawQuery, Result, Sql,
    SqlWriter,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
struct Container {
    #[cfg(not(feature = "disable-intervals"))]
    first: [[Interval; 2]; 1],
    second: [[[Uuid; 1]; 3]; 1],
    third: [[[[IpAddr; 3]; 1]; 3]; 2],
}

pub async fn arrays2(executor: &mut impl Executor) -> Result<()> {
    let _ = MUTEX.lock().await;

    Container::drop_table(executor, true, false).await?;
    Container::create_table(executor, true, true).await?;

    #[cfg(not(feature = "disable-intervals"))]
    let first = [[
        Interval::from_mins(5) + Interval::from_secs(24) + Interval::from_millis(33),
        Interval::from_hours(2) + Interval::from_micros(1),
    ]];
    let second = [[
        [Uuid::from_str("c1412337-cfce-444a-ac46-5e6edcc0bf23").unwrap()],
        [Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap()],
        [Uuid::from_str("9d7f0f5b-19d6-4298-a332-214fc85e2652").unwrap()],
    ]];
    let third = [
        [
            [[
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ]],
            [[
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
                IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
            ]],
            [[
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)),
            ]],
        ],
        [
            [[
                IpAddr::V6(Ipv6Addr::new(
                    0x2001, 0xdb8, 0x3c4d, 0x15, 0, 0, 0x1a2f, 0x1a2b,
                )),
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0x3c4d, 0, 0, 0, 0, 0)),
            ]],
            [[
                IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
                IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)),
            ]],
            [[
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xffff, 0, 0, 0, 0, 1)),
                IpAddr::V4(Ipv4Addr::BROADCAST),
                IpAddr::V6(Ipv6Addr::new(
                    0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                )),
            ]],
        ],
    ];
    let value = Container {
        #[cfg(not(feature = "disable-intervals"))]
        first,
        second,
        third,
    };
    Container::insert_one(executor, &value).await?;
    assert_eq!(Container::find_one(executor, true).await?, Some(value));

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = AnyQuery::default();
        let driver = executor.driver();
        let writer = driver.sql_writer();
        writer.write_drop_table(&mut query, Container::table(), true);
        writer.write_create_table(&mut query, Container::table(), true);
        let value = Container {
            #[cfg(not(feature = "disable-intervals"))]
            first,
            second,
            third,
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
                #[cfg(not(feature = "disable-intervals"))]
                first,
                second,
                third,
            }]
        )
    }
    Ok(())
}
