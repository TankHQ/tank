#![allow(dead_code)]
#![allow(unused_imports)]
use std::{borrow::Cow, pin::pin, sync::LazyLock};
use tank::{
    Driver, Entity, Executor, Query, QueryResult, RawQuery, SqlWriter, cols, stream::TryStreamExt,
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
struct Arrays1 {
    #[cfg(not(feature = "disable-intervals"))]
    aa: [time::Duration; 3],
    bb: Option<[[f32; 4]; 2]>,
    cc: [[[[i8; 1]; 1]; 3]; 1],
    dd: [[[[[i64; 3]; 1]; 1]; 2]; 1],
}

#[derive(Entity, Debug, PartialEq)]
struct Arrays2 {
    alpha: [i32; 5],
    bravo: [[u8; 2]; 3],
    charlie: [[[bool; 2]; 1]; 2],
    delta: [[[[f64; 2]; 2]; 1]; 1],
    echo: [[[Cow<'static, str>; 2]; 2]; 1],
}

pub async fn arrays1<E: Executor>(executor: &mut E) {
    let _ = MUTEX.lock().await;

    // Setup
    Arrays1::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Arrays1 table");
    Arrays2::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Arrays2 table");
    Arrays1::create_table(executor, false, true)
        .await
        .expect("Failed to create Arrays1 table");
    Arrays2::create_table(executor, false, true)
        .await
        .expect("Failed to create Arrays2 table");

    Arrays1::insert_many(
        executor,
        &[
            Arrays1 {
                #[cfg(not(feature = "disable-intervals"))]
                aa: [
                    time::Duration::seconds(1),
                    time::Duration::seconds(2),
                    time::Duration::seconds(3),
                ],
                bb: None,
                cc: [[[[10]], [[20]], [[30]]]],
                dd: [[[[[100, 200, 300]]], [[[400, 500, 600]]]]],
            },
            Arrays1 {
                #[cfg(not(feature = "disable-intervals"))]
                aa: [
                    time::Duration::milliseconds(150),
                    time::Duration::milliseconds(250),
                    time::Duration::milliseconds(350),
                ],
                bb: [[9.9, 8.8, 7.7, 6.6], [5.5, 4.4, 3.3, 2.2]].into(),
                cc: [[[[1]], [[2]], [[3]]]],
                dd: [[[[[7, 8, 9]]], [[[10, 11, 12]]]]],
            },
        ],
    )
    .await
    .expect("Could not insert Arrays1 values");
    {
        let mut stream = pin!(Arrays1::find_many(executor, &true, None));
        while let Some(value) = stream
            .try_next()
            .await
            .expect("Failed to retrieve the value")
        {
            if value.bb.is_none() {
                assert_eq!(
                    value,
                    Arrays1 {
                        #[cfg(not(feature = "disable-intervals"))]
                        aa: [
                            time::Duration::seconds(1),
                            time::Duration::seconds(2),
                            time::Duration::seconds(3),
                        ],
                        bb: None,
                        cc: [[[[10]], [[20]], [[30]]]],
                        dd: [[[[[100, 200, 300]]], [[[400, 500, 600]]]]],
                    }
                );
            } else {
                assert_eq!(
                    value,
                    Arrays1 {
                        #[cfg(not(feature = "disable-intervals"))]
                        aa: [
                            time::Duration::milliseconds(150),
                            time::Duration::milliseconds(250),
                            time::Duration::milliseconds(350),
                        ],
                        bb: [[9.9, 8.8, 7.7, 6.6], [5.5, 4.4, 3.3, 2.2]].into(),
                        cc: [[[[1]], [[2]], [[3]]]],
                        dd: [[[[[7, 8, 9]]], [[[10, 11, 12]]]]],
                    }
                );
            }
        }
    }

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = RawQuery::default();
        let writer = executor.driver().sql_writer();
        writer.write_drop_table::<Arrays1>(&mut query, true);
        writer.write_create_table::<Arrays1>(&mut query, true);
        writer.write_drop_table::<Arrays2>(&mut query, true);
        writer.write_create_table::<Arrays2>(&mut query, true);
        let value = Arrays1 {
            #[cfg(not(feature = "disable-intervals"))]
            aa: [
                time::Duration::seconds(1),
                time::Duration::seconds(2),
                time::Duration::seconds(3),
            ],
            bb: [
                [1.1, -2.2, -3.3, 4.4],
                [5.5, f32::INFINITY, f32::NEG_INFINITY, 8.8],
            ]
            .into(),
            cc: [[[[1]], [[2]], [[3]]]],
            dd: [[[[[10, 20, 30]]], [[[40, 50, 60]]]]],
        };
        writer.write_insert(&mut query, &[value], false);
        writer.write_select(&mut query, cols!(*), Arrays1::table(), &true, None);
        let value = Arrays2 {
            alpha: [1, 2, 3, 4, 5],
            bravo: [[10, 11], [12, 13], [14, 15]],
            charlie: [[[true, false]], [[false, true]]],
            delta: [[[[1.1, 1.2], [2.1, 2.2]]]],
            echo: [[
                [Cow::Borrowed("hello"), Cow::Owned("world".to_string())],
                [Cow::Owned("foo".to_string()), Cow::Borrowed("bar")],
            ]],
        };
        writer.write_insert(&mut query, &[value], false);
        writer.write_select(
            &mut query,
            [Arrays2::alpha, Arrays2::bravo, Arrays2::charlie],
            Arrays2::table(),
            &true,
            None,
        );
        writer.write_select(
            &mut query,
            [Arrays2::delta, Arrays2::echo],
            Arrays2::table(),
            &true,
            None,
        );
        let rows = pin!(executor.run(query).try_filter_map(|v| async move {
            Ok(match v {
                QueryResult::Row(v) => Some(v),
                QueryResult::Affected(..) => None,
            })
        }));
        let rows = rows
            .try_collect::<Vec<_>>()
            .await
            .expect("Could not collect the rows");

        let value = Arrays1::from_row(rows[0].clone()).expect("First must be Arrays1");
        assert_eq!(
            value,
            Arrays1 {
                #[cfg(not(feature = "disable-intervals"))]
                aa: [
                    time::Duration::seconds(1),
                    time::Duration::seconds(2),
                    time::Duration::seconds(3),
                ],
                bb: [
                    [1.1, -2.2, -3.3, 4.4],
                    [5.5, f32::INFINITY, f32::NEG_INFINITY, 8.8],
                ]
                .into(),
                cc: [[[[1]], [[2]], [[3]]]],
                dd: [[[[[10, 20, 30]]], [[[40, 50, 60]]]]],
            },
        );
    }
}
