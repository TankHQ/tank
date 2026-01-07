use crate::ambiguity::{
    first_schema::FirstTableColumnTrait, second_schema::SecondTableColumnTrait,
};
use std::sync::{Arc, LazyLock};
use tank::{
    Entity, Executor, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

mod first_schema {
    use std::sync::Arc;
    use tank::Entity;

    #[derive(Debug, Entity, PartialEq)]
    #[tank(schema = "first_schema")]
    pub struct FirstTable {
        #[tank(primary_key)]
        pub first_col: String,
        #[cfg(not(feature = "disable-large-integers"))]
        pub second_col: Option<u128>,
    }
    #[derive(Debug, Entity, PartialEq)]
    #[tank(schema = "first_schema")]
    pub struct SecondTable {
        #[tank(primary_key)]
        pub first_col: String,
        pub second_col: Option<f64>,
        pub third_col: Option<Arc<u8>>,
    }
}
mod second_schema {
    use std::sync::Arc;
    use tank::Entity;

    #[derive(Debug, Entity, PartialEq)]
    #[tank(schema = "second_schema")]
    pub struct FirstTable {
        #[tank(primary_key)]
        pub first_col: String,
        #[cfg(not(feature = "disable-large-integers"))]
        pub second_col: Option<u128>,
    }
    #[derive(Debug, Entity, PartialEq)]
    #[tank(schema = "second_schema")]
    pub struct SecondTable {
        #[tank(primary_key)]
        pub first_col: String,
        pub second_col: Option<f64>,
        pub third_col: Option<Arc<u8>>,
    }
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn ambiguity<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    first_schema::FirstTable::drop_table(executor, true, false)
        .await
        .expect("Could not drop the first_schema::FirstTable table");
    first_schema::FirstTable::create_table(executor, false, true)
        .await
        .expect("Could not create the first_schema::FirstTable table");
    first_schema::SecondTable::drop_table(executor, true, false)
        .await
        .expect("Could not drop the first_schema::SecondTable table");
    first_schema::SecondTable::create_table(executor, false, true)
        .await
        .expect("Could not create the first_schema::SecondTable table");

    second_schema::FirstTable::drop_table(executor, true, false)
        .await
        .expect("Could not drop the second_schema::FirstTable table");
    second_schema::FirstTable::create_table(executor, false, true)
        .await
        .expect("Could not create the second_schema::FirstTable table");
    second_schema::SecondTable::drop_table(executor, true, false)
        .await
        .expect("Could not drop the second_schema::SecondTable table");
    second_schema::SecondTable::create_table(executor, false, true)
        .await
        .expect("Could not create the second_schema::SecondTable table");

    // Insert one value in first_schema::FirstTable
    first_schema::FirstTable {
        first_col: "a value".into(),
        #[cfg(not(feature = "disable-large-integers"))]
        second_col: 83721.into(),
    }
    .save(executor)
    .await
    .expect("Could not save the first entity");
    assert_eq!(
        first_schema::FirstTable::find_many(
            executor,
            expr!(first_schema::FirstTable::first_col == "a value"),
            None
        )
        .try_collect::<Vec<_>>()
        .await
        .expect(""),
        [first_schema::FirstTable {
            first_col: "a value".into(),
            #[cfg(not(feature = "disable-large-integers"))]
            second_col: 83721.into(),
        }]
    );
    assert_eq!(
        first_schema::FirstTable::find_many(executor, true, None)
            .count()
            .await,
        1
    );
    assert_eq!(
        first_schema::SecondTable::find_many(executor, true, None)
            .count()
            .await,
        0
    );
    assert_eq!(
        second_schema::FirstTable::find_many(executor, true, None)
            .count()
            .await,
        0
    );
    assert_eq!(
        second_schema::SecondTable::find_many(executor, true, None)
            .count()
            .await,
        0
    );

    // Insert one value in second_schema::SecondTable
    second_schema::FirstTable::insert_one(
        executor,
        &second_schema::SecondTable {
            first_col: "another_value".into(),
            second_col: None,
            third_col: Some(Arc::new(19)),
        },
    )
    .await
    .expect("Could not insert second_schema::SecondTable");
    assert_eq!(
        second_schema::SecondTable::find_many(
            executor,
            expr!(second_schema::SecondTable::first_col == "another_value"),
            None
        )
        .try_collect::<Vec<_>>()
        .await
        .expect(""),
        [second_schema::SecondTable {
            first_col: "another_value".into(),
            second_col: None,
            third_col: Some(Arc::new(19)),
        }]
    );
    assert_eq!(
        first_schema::FirstTable::find_many(executor, true, None)
            .count()
            .await,
        1
    );
    assert_eq!(
        first_schema::SecondTable::find_many(executor, true, None)
            .count()
            .await,
        0
    );
    assert_eq!(
        second_schema::FirstTable::find_many(executor, true, None)
            .count()
            .await,
        0
    );
    assert_eq!(
        second_schema::SecondTable::find_many(executor, true, None)
            .count()
            .await,
        1
    );
}
