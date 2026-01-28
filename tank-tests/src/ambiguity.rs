use crate::ambiguity::{
    first_schema::FirstTableColumnTrait as _, first_schema::SecondTableColumnTrait as _,
    second_schema::SecondTableColumnTrait as _,
};
use std::sync::{Arc, LazyLock};
use tank::{
    Entity, Executor, expr,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
    let insert_result = second_schema::SecondTable::insert_one(
        executor,
        &second_schema::SecondTable {
            first_col: "another_value".into(),
            second_col: None,
            third_col: Some(Arc::new(19)),
        },
    )
    .await
    .expect("Could not insert second_schema::SecondTable");
    if let Some(affected) = insert_result.rows_affected {
        assert_eq!(affected, 1);
    }
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

    // Fetch, modify and save the inserted row
    let mut fetched = second_schema::SecondTable::find_one(
        executor,
        expr!(second_schema::SecondTable::first_col == "another_value"),
    )
    .await
    .expect("Failed to fetch inserted second_schema::SecondTable")
    .expect("Inserted row not found");
    assert_eq!(fetched.third_col, Some(Arc::new(19)));
    fetched.third_col = Some(Arc::new(20));
    fetched
        .save(executor)
        .await
        .expect("Could not save updated second_schema::SecondTable");
    let fetched2 = second_schema::SecondTable::find_one(
        executor,
        expr!(second_schema::SecondTable::first_col == "another_value"),
    )
    .await
    .expect("Failed to refetch second_schema::SecondTable")
    .expect("Refetched row not found");
    assert_eq!(fetched2.third_col, Some(Arc::new(20)));
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

    // Insert a row with the same primary key into the other schema to ensure no collisions
    let insert_result2 = first_schema::SecondTable::insert_one(
        executor,
        &first_schema::SecondTable {
            first_col: "another_value".into(),
            second_col: Some(3.14),
            third_col: Some(Arc::new(21)),
        },
    )
    .await
    .expect("Could not insert first_schema::SecondTable");
    if let Some(affected) = insert_result2.rows_affected {
        assert_eq!(affected, 1);
    }
    // Ensure both tables now report one row each
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
        1
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

    // Verify the inserted row in the first schema is independent
    let fs = first_schema::SecondTable::find_one(
        executor,
        expr!(first_schema::SecondTable::first_col == "another_value"),
    )
    .await
    .expect("Failed to fetch first_schema::SecondTable")
    .expect("Row in first_schema::SecondTable not found");
    assert_eq!(fs.third_col, Some(Arc::new(21)));

    // Verify save works for PK-only entities
    first_schema::FirstTable::delete_many(executor, true)
        .await
        .expect("Failed to clear first_schema::FirstTable");
    #[allow(unused_mut)]
    let mut entity = first_schema::FirstTable {
        first_col: "pk_only".into(),
        #[cfg(not(feature = "disable-large-integers"))]
        second_col: None,
    };
    entity.save(executor).await.expect("Failed to save entity");
    #[cfg(not(feature = "disable-large-integers"))]
    {
        entity.second_col = Some(12345678901234567890u128);
    }
    entity
        .save(executor)
        .await
        .expect("Failed to update entity");
    assert_eq!(
        first_schema::FirstTable::find_many(
            executor,
            expr!(first_schema::FirstTable::first_col == "pk_only"),
            None
        )
        .count()
        .await,
        1
    );
}
