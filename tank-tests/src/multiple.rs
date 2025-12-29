#![allow(dead_code)]
use std::sync::LazyLock;
use tank::AsValue;
use tank::{Driver, Entity, Executor, QueryResult, SqlWriter, stream::TryStreamExt};
use tokio::sync::Mutex;

#[derive(Debug, Entity, PartialEq)]
struct One {
    a1: u32,
    string: String,
    c1: u64,
}
#[derive(Debug, Entity, PartialEq)]
struct Two {
    a2: u32,
    string: String,
}
#[derive(Debug, Entity, PartialEq)]
struct Three {
    string: String,
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn multiple<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    let mut sql = String::new();
    let writer = executor.driver().sql_writer();
    sql.push_str("    \n\n  \n \n\t\t\n   \n    ");
    // 1
    writer.write_drop_table::<One>(&mut sql, true);
    sql.push_str("\t\t");
    // 2
    writer.write_drop_table::<Two>(&mut sql, true);
    // 3
    writer.write_drop_table::<Three>(&mut sql, true);
    // 4
    writer.write_create_table::<One>(&mut sql, true);
    sql.push('\n');
    // 5
    writer.write_create_table::<Two>(&mut sql, true);
    // 6
    writer.write_create_table::<Three>(&mut sql, true);
    sql.push_str(" ");
    // 7
    writer.write_insert(
        &mut sql,
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
    sql.push_str("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
    // 8
    writer.write_insert(
        &mut sql,
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
    writer.write_select(&mut sql, [Three::string], Three::table(), &true, None);
    // 10
    writer.write_insert(
        &mut sql,
        [&One {
            a1: 11,
            string: "zzz".into(),
            c1: 512,
        }],
        false,
    );
    // 11
    writer.write_select(
        &mut sql,
        [One::a1, One::string, One::c1],
        One::table(),
        &true,
        None,
    );
    // 12
    writer.write_select(&mut sql, [Two::a2, Two::string], Two::table(), &true, None);
    sql.push_str("            \t    \t\t  \n \n \n \t    \n\n\n ");
    let result = executor
        .run(sql)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not run the composite query");
    // 12 statements but one select returns 3 rows and another one returns 2 rows (12 - 2 + 3 + 2 = 15)
    assert_eq!(result.len(), 15);
    let mut result = result
        .into_iter()
        .filter_map(|v| match v {
            QueryResult::Row(row) => Some(row),
            QueryResult::Affected(..) => None,
        })
        .collect::<Vec<_>>();
    result.sort_by(|a, b| {
        let a = a
            .get_column("string")
            .map(|v| String::try_from_value(v.clone()))
            .expect("Does not have column \"string\"")
            .expect("The column called `string` is not a VARCHAR");
        let b = b
            .get_column("string")
            .map(|v| String::try_from_value(v.clone()))
            .expect("Does not have column \"string\"")
            .expect("The column called `string` is not a VARCHAR");
        a.cmp(&b)
    });
    assert_eq!(result.len(), 6);
    let mut result = result.into_iter().peekable();
    assert_eq!(*result.peek().unwrap().labels, ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        Two {
            a2: 21,
            string: "aaa".into()
        }
    );
    result.next();
    assert_eq!(*result.peek().unwrap().labels, ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        Two {
            a2: 22,
            string: "bbb".into()
        }
    );
    result.next();
    assert_eq!(*result.peek().unwrap().labels, ["string"]);
    assert_eq!(
        Three::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        Three {
            string: "ccc".into(),
        }
    );
    result.next();
    assert_eq!(*result.peek().unwrap().labels, ["string"]);
    assert_eq!(
        Three::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        Three {
            string: "ddd".into(),
        }
    );
    result.next();
    assert_eq!(*result.peek().unwrap().labels, ["a2", "string"]);
    assert_eq!(
        Two::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        Two {
            a2: 23,
            string: "eee".into()
        }
    );
    result.next();
    assert_eq!(*result.peek().unwrap().labels, ["a1", "string", "c1"]);
    assert_eq!(
        One::from_row(result.peek().unwrap().clone()).expect("The row was not an entity Two"),
        One {
            a1: 11,
            string: "zzz".into(),
            c1: 512,
        }
    );
    result.next();
}
