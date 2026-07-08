#![allow(unused_imports)]
use std::{collections::HashMap, pin::pin, sync::LazyLock};
use tank::{
    AsValue, Connection, Driver, DynQuery, Entity, Error, Executor, QueryBuilder, QueryResult,
    Result, RowsAffected, SqlWriter, Transaction, Value, cols, expr, join,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, PartialEq, Clone)]
pub struct Notes(pub String);
pub struct NotesWrap(pub Notes);

impl AsValue for NotesWrap {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        Value::Varchar(Some(self.0.0.into()))
    }
    fn try_from_value(value: Value) -> Result<Self> {
        match value.try_as(&Value::Varchar(None)) {
            Ok(Value::Varchar(Some(s))) => Ok(NotesWrap(Notes(s.to_string()))),
            _ => Err(Error::msg("Expected Varchar for Notes")),
        }
    }
}
impl From<Notes> for NotesWrap {
    fn from(v: Notes) -> Self {
        NotesWrap(v)
    }
}
impl From<NotesWrap> for Notes {
    fn from(v: NotesWrap) -> Self {
        v.0
    }
}

#[derive(Entity, Debug, PartialEq)]
#[tank(
    schema = "army",
    name = "deployments",
    primary_key = (Self::unit_id, Self::region),
)]
struct EntityExample {
    unit_id: Uuid,
    #[tank(clustering_key)]
    region: String,
    #[tank(name = "callsign")]
    callsign: String,
    casualties: i32,
    #[tank(conversion_type = NotesWrap)]
    metadata: Notes,
    #[tank(ignore)]
    transient_cache: HashMap<String, String>,
}

pub async fn cheat_sheet(executor: &mut impl Connection) {
    let _lock = MUTEX.lock().await;

    EntityExample::drop_table(executor, true, false)
        .await
        .expect("Failed to drop EntityExample table");
    EntityExample::create_table(executor, false, true)
        .await
        .expect("Failed to create EntityExample table");

    let uid1 = Uuid::new_v4();
    let uid2 = Uuid::new_v4();
    let uid3 = Uuid::new_v4();

    let entity1 = EntityExample {
        unit_id: uid1,
        region: "North".into(),
        callsign: "Alpha-1".into(),
        casualties: 0,
        metadata: Notes("mission-1".into()),
        transient_cache: HashMap::new(),
    };
    EntityExample::insert_one(executor, &entity1)
        .await
        .expect("Failed to insert one");

    EntityExample::insert_many(
        executor,
        &[EntityExample {
            unit_id: uid2,
            region: "South".into(),
            callsign: "Bravo-2".into(),
            casualties: 3,
            metadata: Notes("mission-2".into()),
            transient_cache: HashMap::new(),
        }],
    )
    .await
    .expect("Failed to insert many");

    executor
        .append(&[EntityExample {
            unit_id: uid3,
            region: "East".into(),
            callsign: "Charlie-3".into(),
            casualties: 5,
            metadata: Notes("mission-3".into()),
            transient_cache: HashMap::new(),
        }])
        .await
        .expect("append");

    let entity = EntityExample::find_one(executor, entity1.primary_key_expr())
        .await
        .expect("Failed to query find one")
        .expect("Expected one result");
    assert_eq!(entity.callsign, "Alpha-1");
    assert_eq!(entity.metadata, Notes("mission-1".into()));

    #[cfg(not(feature = "disable-scanning"))]
    {
        let mut n = 0usize;
        let mut stream = pin!(EntityExample::find_many(
            executor,
            expr!(EntityExample::casualties > 0),
            Some(100),
        ));
        while let Some(entity) = stream.try_next().await.expect("stream next") {
            assert!(entity.casualties > 0);
            n += 1;
        }
        assert_eq!(n, 2);
    }
    #[cfg(feature = "disable-scanning")]
    {
        let mut n = 0usize;
        let mut stream = pin!(EntityExample::find_many(executor, true, Some(100)));
        while stream.try_next().await.expect("stream next").is_some() {
            n += 1;
        }
        assert_eq!(n, 3);
    }

    let uid = uid1;
    let entities = EntityExample::find_many(executor, expr!(EntityExample::unit_id == #uid), None)
        .try_collect::<Vec<EntityExample>>()
        .await
        .expect("collect by uid");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0].callsign, "Alpha-1");

    let mut alpha = EntityExample::find_one(executor, entity1.primary_key_expr())
        .await
        .expect("find for save")
        .expect("present for save");
    alpha.casualties = 7;
    alpha.save(executor).await.expect("save");
    let saved = EntityExample::find_one(executor, entity1.primary_key_expr())
        .await
        .expect("find after save")
        .expect("present after save");
    assert_eq!(saved.casualties, 7);

    let bravo = EntityExample::find_one(
        executor,
        expr!(EntityExample::unit_id == #uid2 && EntityExample::region == "South"),
    )
    .await
    .expect("find Bravo for delete")
    .expect("Bravo present");
    bravo.delete(executor).await.expect("entity delete");
    assert!(
        EntityExample::find_one(
            executor,
            expr!(EntityExample::unit_id == #uid2 && EntityExample::region == "South")
        )
        .await
        .expect("find after delete")
        .is_none(),
        "Bravo-2 must be gone after delete"
    );

    let uid4 = Uuid::new_v4();
    EntityExample::insert_one(
        executor,
        &EntityExample {
            unit_id: uid4,
            region: "West".into(),
            callsign: "Delta-4".into(),
            casualties: 0,
            metadata: Notes("tmp".into()),
            transient_cache: HashMap::new(),
        },
    )
    .await
    .expect("insert Delta-4");

    #[cfg(not(feature = "disable-scanning"))]
    {
        EntityExample::delete_many(executor, expr!(EntityExample::casualties == 0))
            .await
            .expect("delete_many literal");
        assert!(
            EntityExample::find_one(
                executor,
                expr!(EntityExample::unit_id == #uid4 && EntityExample::region == "West")
            )
            .await
            .expect("find Delta-4 after delete_many")
            .is_none(),
            "Delta-4 (casualties=0) must be gone after delete_many"
        );
    }
    #[cfg(feature = "disable-scanning")]
    {
        EntityExample::delete_many(
            executor,
            expr!(EntityExample::unit_id == #uid4 && EntityExample::region == "West"),
        )
        .await
        .expect("delete Delta-4 by PK");
    }

    let uid = uid3;
    EntityExample::delete_many(executor, expr!(EntityExample::unit_id == #uid))
        .await
        .expect("delete_many variable");
    assert!(
        EntityExample::find_one(
            executor,
            expr!(EntityExample::unit_id == #uid3 && EntityExample::region == "East")
        )
        .await
        .expect("find Charlie after delete_many")
        .is_none(),
        "Charlie-3 must be gone after delete_many"
    );

    let uid = uid1;
    let _ = EntityExample::find_many(executor, expr!(EntityExample::unit_id == #uid), None)
        .try_collect::<Vec<_>>()
        .await
        .expect("expr uid == #uid");

    #[cfg(not(feature = "disable-scanning"))]
    {
        let _ = EntityExample::find_many(executor, expr!(EntityExample::casualties == 0), None)
            .try_collect::<Vec<_>>()
            .await
            .expect("expr casualties == 0");

        let _ = EntityExample::find_many(executor, expr!(EntityExample::casualties >= 10), None)
            .try_collect::<Vec<_>>()
            .await
            .expect("expr casualties >= 10");

        let _ = EntityExample::find_many(
            executor,
            expr!(EntityExample::region == "North" || EntityExample::region == "South"),
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("expr OR");

        let _ = EntityExample::find_many(
            executor,
            expr!(EntityExample::callsign == "Alpha%" as LIKE),
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("expr LIKE");

        let _ = EntityExample::find_many(
            executor,
            expr!(EntityExample::callsign != "Alpha%" as LIKE),
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("expr NOT LIKE");
    }

    let uid5 = Uuid::new_v4();
    EntityExample::insert_one(
        executor,
        &EntityExample {
            unit_id: uid5,
            region: "PrepRegion".into(),
            callsign: "Echo-5".into(),
            casualties: 20,
            metadata: Notes("prep".into()),
            transient_cache: HashMap::new(),
        },
    )
    .await
    .expect("insert Echo-5");

    let mut query =
        EntityExample::prepare_find(executor, expr!(EntityExample::unit_id == ?), Some(50))
            .await
            .expect("prepare_find");
    query.bind(uid5).expect("bind uid5");
    let prepared_results = executor
        .fetch(&mut query)
        .map_ok(|row| EntityExample::from_row(row).unwrap())
        .try_collect::<Vec<EntityExample>>()
        .await
        .expect("fetch prepared");
    assert_eq!(prepared_results.len(), 1);
    assert_eq!(prepared_results[0].callsign, "Echo-5");

    query.clear_bindings().expect("clear bindings");
    query.bind(Uuid::nil()).expect("bind nil");
    let empty_results = executor
        .fetch(&mut query)
        .map_ok(|row| EntityExample::from_row(row).unwrap())
        .try_collect::<Vec<EntityExample>>()
        .await
        .expect("fetch prepared reuse");
    assert!(empty_results.is_empty(), "nil UUID must return nothing");

    #[cfg(not(feature = "disable-scanning"))]
    {
        let results = executor
            .fetch(
                QueryBuilder::new()
                    .select(EntityExample::columns())
                    .from(EntityExample::table())
                    .where_expr(expr!(EntityExample::casualties > 0))
                    .order_by(cols!(EntityExample::casualties DESC))
                    .limit(Some(50))
                    .build(&executor.driver()),
            )
            .map_ok(|row| EntityExample::from_row(row).unwrap())
            .try_collect::<Vec<_>>()
            .await
            .expect("query builder ordering");
        assert!(!results.is_empty());
        if results.len() > 1 {
            assert!(results[0].casualties >= results[1].casualties);
        }
    }

    {
        let mut stream = pin!(executor.run("SELECT unit_id, callsign FROM army.deployments"));
        let mut found_row = false;
        while let Some(result) = stream.try_next().await.expect("run stream") {
            if matches!(result, QueryResult::Row(_)) {
                found_row = true;
            }
        }
        assert!(found_row, "run must produce at least one Row result");
    }

    {
        let rows: Vec<_> = executor
            .fetch("SELECT * FROM army.deployments")
            .try_collect()
            .await
            .expect("fetch raw");
        assert!(!rows.is_empty());
    }

    #[cfg(not(feature = "disable-scanning"))]
    {
        let affected = executor
            .execute("UPDATE army.deployments SET casualties = 0 WHERE region = 'North'")
            .await
            .expect("execute raw update");
        let _ = affected;
    }

    let mut raw_query = executor
        .prepare(
            QueryBuilder::new()
                .select(EntityExample::columns())
                .from(EntityExample::table())
                .where_expr(expr!(EntityExample::unit_id == ?))
                .build(&executor.driver()),
        )
        .await
        .expect("prepare raw query");

    raw_query.bind(uid5).expect("bind uid5 raw");
    let raw_results: Vec<EntityExample> = executor
        .fetch(&mut raw_query)
        .map_ok(|row| EntityExample::from_row(row).unwrap())
        .try_collect()
        .await
        .expect("raw prepared fetch");
    assert_eq!(raw_results.len(), 1);

    raw_query.clear_bindings().expect("clear raw bindings");
    raw_query.bind(Uuid::nil()).expect("bind nil raw");
    let raw_empty: Vec<EntityExample> = executor
        .fetch(&mut raw_query)
        .map_ok(|row| EntityExample::from_row(row).unwrap())
        .try_collect()
        .await
        .expect("raw prepared reuse");
    assert!(raw_empty.is_empty());

    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let writer = executor.driver().sql_writer();
        let mut query = DynQuery::default();
        writer.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(EntityExample::columns())
                .from(EntityExample::table())
                .where_expr(true)
                .limit(Some(10)),
        );
        let results: Vec<QueryResult> = executor
            .run(query)
            .try_collect()
            .await
            .expect("sqlwriter run");
        assert!(
            results.iter().any(|r| matches!(r, QueryResult::Row(..))),
            "SqlWriter must stream at least one Row"
        );
    }

    #[cfg(not(feature = "disable-transactions"))]
    {
        let uid6 = Uuid::new_v4();
        let echo = EntityExample {
            unit_id: uid6,
            region: "TxRegion".into(),
            callsign: "Foxtrot-6".into(),
            casualties: 0,
            metadata: Notes("tx".into()),
            transient_cache: HashMap::new(),
        };

        let mut tx = executor.begin().await.expect("begin tx");

        echo.save(&mut tx).await.expect("save in tx");

        EntityExample::delete_many(&mut tx, expr!(EntityExample::unit_id == #uid5))
            .await
            .expect("delete_many in tx");

        tx.commit().await.expect("commit tx");

        assert!(
            EntityExample::find_one(executor, echo.primary_key_expr())
                .await
                .expect("find after commit")
                .is_some(),
            "entity saved in committed transaction must be visible"
        );

        assert!(
            EntityExample::find_one(
                executor,
                expr!(EntityExample::unit_id == #uid5 && EntityExample::region == "PrepRegion")
            )
            .await
            .expect("find Echo-5 after commit")
            .is_none(),
            "entity deleted in committed transaction must be gone"
        );
    }
}
