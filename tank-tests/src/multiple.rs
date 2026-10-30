#![allow(dead_code)]
use std::sync::LazyLock;
use tank::{AsValue, DynQuery, QueryBuilder, SqlWriter};
use tank::{Driver, Entity, Executor, QueryResult, Row, cols, expr, stream::TryStreamExt};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug)]
struct One {
    a1: u32,
    string: String,
    c1: u64,
}

#[derive(Entity, PartialEq, Debug)]
struct Two {
    a2: u32,
    string: String,
}

#[derive(Entity, PartialEq, Debug)]
struct Three {
    string: String,
}

pub async fn multiple(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    let mut query = DynQuery::default();
    let writer = executor.driver().sql_writer();
    query.push_str("    \n\n  \n \n\t\t\n   \n    ");
    // 1
    writer.write_drop_table::<One>(&mut query, true);
    query.push_str("\t\t");
    // 2
    writer.write_drop_table::<Two>(&mut query, true);
    // 3
    writer.write_drop_table::<Three>(&mut query, true);
    // 4
    writer.write_create_table::<One>(&mut query, true);
    query.push('\n');
    // 5
    writer.write_create_table::<Two>(&mut query, true);
    // 6
    writer.write_create_table::<Three>(&mut query, true);
    query.push_str(" ");
    // 7
    writer.write_insert(
        &mut query,
        [
            &Two {
                a2: 21,
                string: "aaa".into(),
            },
            &Two {
                a2: 22,
                string: "bbb".into(),
            },
            &Two {
                a2: 23,
                string: "eee".into(),
            },
        ],
        false,
    );
    query.push_str("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
    // 8
    writer.write_insert(
        &mut query,
        [
            &Three {
                string: "ddd".into(),
            },
            &Three {
                string: "ccc".into(),
            },
        ],
        false,
    );
    // 9
    writer.write_select(
        &mut query,
        &QueryBuilder::new()
            .select([Three::string])
            .from(Three::table())
            .where_expr(true)
            .order_by(cols!(Three::string ASC)),
    );
    // 10
    writer.write_insert(
        &mut query,
        [&One {
            a1: 11,
            string: "zzz".into(),
            c1: 512,
        }],
        false,
    );
    // 11
    writer.write_select(
        &mut query,
        &QueryBuilder::new()
            .select([One::a1, One::string, One::c1])
            .from(One::table())
            .where_expr(true)
            .order_by(cols!(One::string ASC)),
    );
    // 12
    writer.write_select(
        &mut query,
        &QueryBuilder::new()
            .select([Two::a2, Two::string])
            .from(Two::table())
            .where_expr(true)
            .order_by(cols!(Two::string ASC)),
    );
    // 13: An empty SELECT still contributes a statement result.
    writer.write_select(
        &mut query,
        &QueryBuilder::new()
            .select([Two::a2, Two::string])
            .from(Two::table())
            .where_expr(false),
    );
    // 14: A write that affects no rows must not disappear either.
    writer.write_delete::<Two>(&mut query, expr!(Two::a2 == 0));
    query.push_str("            \t    \t\t  \n \n \n \t    \n\n\n ");
    let mut query = <DynQuery as tank::AsQuery<_>>::as_query(query);
    let result = executor
        .run(&mut query)
        .map_ok(|result| match result {
            QueryResult::Row(row) => QueryResult::Row(row.into_owned()),
            QueryResult::Affected(affected) => QueryResult::Affected(affected),
        })
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // 14 statements: 8 writes + 2 rows + 1 write + 1 row + 3 rows + 0 rows + 1 write = 16 items
    assert_eq!(result.len(), 16);
    for (index, result) in result.iter().enumerate() {
        assert_eq!(
            matches!(result, QueryResult::Row(_)),
            [8, 9, 11, 12, 13, 14].contains(&index),
            "Unexpected result kind at index {}",
            index,
        );
    }
    let result = result
        .into_iter()
        .enumerate()
        .filter_map(|(index, result)| match result {
            QueryResult::Row(row) => Some(row),
            QueryResult::Affected(affected) => {
                if index == 15 {
                    if let Some(count) = affected.rows_affected {
                        assert_eq!(count, 0);
                    }
                }
                None
            }
        })
        .collect::<Vec<_>>();
    let expected = result
        .iter()
        .map(|row| String::try_from_value(row.get_column("string").unwrap()).unwrap())
        .collect::<Vec<_>>();
    drop(result);
    let mut result = executor
        .fetch(&mut query)
        .map_ok(Row::into_owned)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let projected = result
        .iter()
        .map(|row| String::try_from_value(row.get_column("string").unwrap()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        projected, expected,
        "Fetch flattens all statement rowsets in order"
    );
    result.sort_by(|a, b| {
        let a = a
            .get_column("string")
            .map(String::try_from_value)
            .expect("Does not have column \"string\"")
            .expect("The column called `string` is not a VARCHAR");
        let b = b
            .get_column("string")
            .map(String::try_from_value)
            .expect("Does not have column \"string\"")
            .expect("The column called `string` is not a VARCHAR");
        a.cmp(&b)
    });
    assert_eq!(result.len(), 6);
    let mut result = result.into_iter().peekable();
    assert_eq!(result.peek().unwrap().names(), ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        Two {
            a2: 21,
            string: "aaa".into()
        }
    );
    result.next();
    assert_eq!(result.peek().unwrap().names(), ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        Two {
            a2: 22,
            string: "bbb".into()
        }
    );
    result.next();
    assert_eq!(result.peek().unwrap().names(), ["string"]);
    assert_eq!(
        Three::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        Three {
            string: "ccc".into(),
        }
    );
    result.next();
    assert_eq!(result.peek().unwrap().names(), ["string"]);
    assert_eq!(
        Three::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        Three {
            string: "ddd".into(),
        }
    );
    result.next();
    assert_eq!(result.peek().unwrap().names(), ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        Two {
            a2: 23,
            string: "eee".into()
        }
    );
    result.next();
    assert_eq!(result.peek().unwrap().names(), ["a1", "string", "c1"]);
    assert_eq!(
        One::from_row(result.peek().unwrap()).expect("The row was not an entity Two"),
        One {
            a1: 11,
            string: "zzz".into(),
            c1: 512,
        }
    );
    result.next();
    assert!(result.next().is_none());
    drop(result);
    let affected = executor.execute(&mut query).await.unwrap();
    if let Some(count) = affected.rows_affected {
        assert_eq!(count, 6, "Execute counts writes, not the six selected rows");
    }
}
