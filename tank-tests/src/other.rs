use crate::silent_logs;
use std::{pin::pin, sync::LazyLock};
use tank::{
    AnyQuery, AsValue, Driver, Entity, Executor, Result, Row, Sql, cols, expr, stream::TryStreamExt,
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug)]
pub struct ATable {
    #[tank(primary_key)]
    a_column: String,
}

#[derive(Entity, PartialEq, Debug)]
pub struct CharTable {
    #[tank(primary_key)]
    id: i32,
    letter: char,
}

pub async fn other(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    silent_logs! {
        ATable::drop_table(executor, true, false).await?;
    }
    ATable::create_table(executor, true, true).await?;
    ATable::insert_one(
        executor,
        &ATable {
            a_column: "".into(),
        },
    )
    .await?;

    // SELECT NULL
    let stream = executor
        .fetch(
            Sql()
                .select(cols!(NULL))
                .from(ATable::table())
                .where_expr(true)
                .build(&executor.driver()),
        )
        .map_ok(|v| v.value(0).unwrap().as_value().into_owned());
    let value = pin!(stream)
        .try_next()
        .await?
        .expect("No result returned from the stream");
    assert!(value.is_null());

    // InsertIntoQueryBuilder::build and write_into should produce identical SQL
    let rows = vec![ATable {
        a_column: "test".into(),
    }];
    let query = Sql().insert_into(ATable::table()).values(&rows);
    let from_build = query.build(&executor.driver());
    let mut out = AnyQuery::default();
    query.write_into(&executor.driver().sql_writer(), &mut out);
    assert_eq!(
        from_build.as_str(),
        out.as_str(),
        "build() and write_into() should produce the same SQL"
    );

    // Multi byte char
    silent_logs! {
        CharTable::drop_table(executor, true, false).await?;
    }
    CharTable::create_table(executor, true, true).await?;
    CharTable::insert_one(
        executor,
        &CharTable {
            id: 1, letter: 'é'
        },
    )
    .await?;
    let row = CharTable::find_one(executor, expr!(CharTable::id == 1))
        .await?
        .expect("Row with id=1 not found");
    assert_eq!(
        row.letter, 'é',
        "Multi-byte char should round-trip correctly"
    );
    Ok(())
}
