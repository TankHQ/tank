use std::pin::pin;
use std::sync::LazyLock;
use tank::stream::StreamExt;
use tank::{AsValue, Entity, Executor, QueryBuilder, Row, cols, expr};
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

pub async fn keywords(executor: &mut impl Executor) {
    let _lock = MUTEX.lock().await;

    // Setup
    ReservedWords::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ReservedWords table");
    ReservedWords::create_table(executor, true, true)
        .await
        .expect("Failed to create ReservedWords table");

    // Query
    let entity = ReservedWords {
        id: 1,
        select_field: "value".to_string(),
        where_field: 42,
        group_field: true,
        order_field: Some("asc".to_string()),
        limit_field: 10.5,
    };
    entity
        .save(executor)
        .await
        .expect("Failed to save entity with reserved keywords");

    let found = ReservedWords::find_one(executor, expr!(ReservedWords::id == 1))
        .await
        .expect("Failed to query")
        .expect("Entity not found");
    assert_eq!(found, entity);

    let found_by_where = ReservedWords::find_one(executor, expr!(ReservedWords::where_field == 42))
        .await
        .expect("Failed to query by reserved keyword field");
    assert!(found_by_where.is_some());
    assert_eq!(found_by_where.unwrap(), entity);

    {
        let mut stream = pin!(
            executor.fetch(
                QueryBuilder::new()
                    .select(cols!(ReservedWords::group_field, COUNT(*)))
                    .from(ReservedWords::table())
                    .group_by(cols!(ReservedWords::group_field))
                    .build(&executor.driver())
            )
        );
        let row = stream
            .next()
            .await
            .expect("Expected a row")
            .expect("Query failed");
        let Row { values, .. } = row;
        let group_val =
            bool::try_from_value(values[0].clone()).expect("Failed to decode group bool");
        let count_val = i64::try_from_value(values[1].clone()).expect("Failed to decode count");
        assert_eq!(group_val, true);
        assert_eq!(count_val, 1);
    }
    {
        let mut ordered_stream = pin!(ReservedWords::find_many(executor, true, None));
        while let Some(res) = ordered_stream.next().await {
            res.expect("Failed while fetching ordered");
        }
    }
    {
        let mut stream_ordered = pin!(
            executor.fetch(
                QueryBuilder::new()
                    .select(ReservedWords::columns())
                    .from(ReservedWords::table())
                    .order_by(cols!(ReservedWords::order_field ASC))
                    .limit(Some(1))
                    .build(&executor.driver())
            )
        );

        let row_ordered = stream_ordered
            .next()
            .await
            .expect("Expected row")
            .expect("Query failed");
        let entity_ordered = ReservedWords::from_row(row_ordered).expect("Failed to parse entity");
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
    entity2.save(executor).await.expect("Saved 2");
    {
        let mut stream_group = pin!(
            executor.fetch(
                QueryBuilder::new()
                    .select(cols!(ReservedWords::group_field, COUNT(*)))
                    .from(ReservedWords::table())
                    .group_by(cols!(ReservedWords::group_field))
                    .order_by(cols!(ReservedWords::group_field ASC))
                    .build(&executor.driver())
            )
        );

        let row1 = stream_group.next().await.unwrap().unwrap();
        let Row { values, .. } = row1;
        let g = bool::try_from_value(values[0].clone()).unwrap();
        assert_eq!(g, false);

        let row2 = stream_group.next().await.unwrap().unwrap();
        let Row { values, .. } = row2;
        let g = bool::try_from_value(values[0].clone()).unwrap();
        assert_eq!(g, true);
    }

    let found_limit = ReservedWords::find_one(executor, expr!(ReservedWords::limit_field > 15.0))
        .await
        .expect("Query limit failed")
        .expect("Limit entity not found");
    assert_eq!(found_limit.id, 2);
}
