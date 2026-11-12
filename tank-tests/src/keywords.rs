use std::pin::pin;
use std::sync::LazyLock;
use tank::stream::TryStreamExt;
use tank::{AsValue, Entity, Executor, Sql, Result, Row, cols, expr};
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, PartialEq)]
#[tank(name = "reserved_table")]
pub struct ReservedWords {
    #[tank(primary_key)]
    pub id: i32,
    #[tank(name = "select")]
    pub select_field: String,
    #[tank(name = "where")]
    pub where_field: i32,
    #[tank(name = "group")]
    pub group_field: bool,
    #[tank(name = "order")]
    pub order_field: Option<String>,
    #[tank(name = "limit")]
    pub limit_field: f64,
}

pub async fn keywords(executor: &mut impl Executor) -> Result<()> {
    let _lock = MUTEX.lock().await;

    // Setup
    ReservedWords::drop_table(executor, true, false).await?;
    ReservedWords::create_table(executor, true, true).await?;

    // Query
    let entity = ReservedWords {
        id: 1,
        select_field: "value".to_string(),
        where_field: 42,
        group_field: true,
        order_field: Some("asc".to_string()),
        limit_field: 10.5,
    };
    entity.save(executor).await?;

    let found = ReservedWords::find_one(executor, expr!(ReservedWords::id == 1))
        .await?
        .expect("Entity not found");
    assert_eq!(found, entity);

    let found_by_where =
        ReservedWords::find_one(executor, expr!(ReservedWords::where_field == 42)).await?;
    assert!(found_by_where.is_some());
    assert_eq!(found_by_where.unwrap(), entity);

    {
        let mut stream = pin!(
            executor.fetch(
                Sql()
                    .select(cols!(ReservedWords::group_field, COUNT(*)))
                    .from(ReservedWords::table())
                    .group_by(cols!(ReservedWords::group_field))
                    .build(&executor.driver())
            )
        );
        let row = stream.try_next().await?.expect("Expected a row");
        let group_val = bool::try_from_value(row.value(0).unwrap())?;
        let count_val = i64::try_from_value(row.value(1).unwrap())?;
        assert_eq!(group_val, true);
        assert_eq!(count_val, 1);
    }
    {
        let mut ordered_stream = pin!(ReservedWords::find_many(executor, true, None));
        while ordered_stream.try_next().await?.is_some() {}
    }
    {
        let mut stream_ordered = pin!(
            executor.fetch(
                Sql()
                    .select(ReservedWords::columns())
                    .from(ReservedWords::table())
                    .order_by(cols!(ReservedWords::order_field ASC))
                    .limit(Some(1))
                    .build(&executor.driver())
            )
        );

        let row_ordered = stream_ordered.try_next().await?.expect("Expected row");
        let entity_ordered = ReservedWords::from_row(row_ordered)?;
        assert_eq!(entity_ordered.order_field.as_deref(), Some("asc"));
    }

    let entity2 = ReservedWords {
        id: 2,
        select_field: "value2".to_string(),
        where_field: 43,
        group_field: false,
        order_field: Some("desc".to_string()),
        limit_field: 20.5,
    };
    entity2.save(executor).await?;
    {
        let mut stream_group = pin!(
            executor.fetch(
                Sql()
                    .select(cols!(ReservedWords::group_field, COUNT(*)))
                    .from(ReservedWords::table())
                    .group_by(cols!(ReservedWords::group_field))
                    .order_by(cols!(ReservedWords::group_field ASC))
                    .build(&executor.driver())
            )
        );

        let row1 = stream_group.try_next().await?.unwrap();
        let g = bool::try_from_value(row1.value(0).unwrap())?;
        assert_eq!(g, false);

        let row2 = stream_group.try_next().await?.unwrap();
        let g = bool::try_from_value(row2.value(0).unwrap())?;
        assert_eq!(g, true);
    }

    let found_limit = ReservedWords::find_one(executor, expr!(ReservedWords::limit_field > 15.0))
        .await?
        .expect("Limit entity not found");
    assert_eq!(found_limit.id, 2);
    Ok(())
}
