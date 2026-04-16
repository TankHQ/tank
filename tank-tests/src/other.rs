use std::{pin::pin, sync::LazyLock};
use tank::{
    DynQuery, Entity, Executor, QueryBuilder, cols, expr,
    stream::{StreamExt, TryStreamExt},
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

pub async fn other(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    // Setup
    ATable::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ATable table");
    ATable::create_table(executor, true, true)
        .await
        .expect("Failed to create ATable table");
    ATable::insert_one(
        executor,
        &ATable {
            a_column: "".into(),
        },
    )
    .await
    .expect("Could not save a row");

    // SELECT NULL
    let stream = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(NULL))
                .from(ATable::table())
                .where_expr(true)
                .build(&executor.driver()),
        )
        .map_ok(|v| v.values.into_iter().nth(0).unwrap());
    let value = pin!(stream)
        .next()
        .await
        .expect("No result returned from the stream")
        .expect("Could not query for NULL");
    assert!(value.is_null());

    // InsertIntoQueryBuilder::build and build_into should produce identical SQL
    let rows = vec![ATable {
        a_column: "test".into(),
    }];
    let query = QueryBuilder::new().insert_into::<ATable>().values(&rows);
    let from_build = query.build(&executor.driver());
    let mut out = DynQuery::default();
    query.build_into(&executor.driver(), &mut out);
    let from_build_into: String = out.into();
    assert_eq!(
        from_build, from_build_into,
        "build() and build_into() should produce the same SQL"
    );

    // Multi byte char
    CharTable::drop_table(executor, true, false)
        .await
        .expect("Failed to drop CharTable");
    CharTable::create_table(executor, true, true)
        .await
        .expect("Failed to create CharTable");
    CharTable::insert_one(
        executor,
        &CharTable {
            id: 1, letter: 'é'
        },
    )
    .await
    .expect("Could not insert multi-byte char");
    let row = CharTable::find_one(executor, expr!(CharTable::id == 1))
        .await
        .expect("Failed to query CharTable")
        .expect("Row with id=1 not found");
    assert_eq!(
        row.letter, 'é',
        "Multi-byte char should round-trip correctly"
    );
}
