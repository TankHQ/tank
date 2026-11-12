#![allow(unused_variables)]
use std::{collections::HashMap, future, sync::LazyLock};
use tank::{AsValue, Entity, Result, Value, ValueOwned, anyhow, expr, stream::StreamExt};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// #region cheat-sheet-conversion
#[derive(Clone)]
pub struct Notes(pub String); // Third party type
pub struct NotesWrap(pub Notes); // Local wrapper

impl AsValue for NotesWrap {
    fn as_empty_value() -> ValueOwned {
        Value::Varchar(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(self.0.0.into()))
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        match value.as_value().try_as(&Value::Varchar(None)) {
            Ok(Value::Varchar(Some(s))) => Ok(NotesWrap(Notes(s.to_string()))),
            _ => Err(anyhow!("Expected Varchar for Notes")),
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
// #endregion cheat-sheet-conversion

// #region cheat-sheet-entity
#[derive(Entity)]
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
// #endregion cheat-sheet-entity

pub async fn cheat_sheet(mut connection: &mut impl tank::Connection) -> Result<()> {
    let _lock = MUTEX.lock().await;

    {
        EntityExample::drop_table(&mut connection, true, false).await?;
        EntityExample::create_table(&mut connection, true, true).await?;
    }

    let entity = EntityExample {
        unit_id: Uuid::new_v4(),
        region: "North".into(),
        callsign: "Alpha".into(),
        casualties: 0,
        metadata: Notes("mission".into()),
        transient_cache: HashMap::new(),
    };

    let entity2 = EntityExample {
        unit_id: Uuid::new_v4(),
        region: "South".into(),
        callsign: "Bravo-2".into(),
        casualties: 3,
        metadata: Notes("mission-2".into()),
        transient_cache: HashMap::new(),
    };
    let entity3 = EntityExample {
        unit_id: Uuid::new_v4(),
        region: "East".into(),
        callsign: "Charlie-3".into(),
        casualties: 5,
        metadata: Notes("mission-3".into()),
        transient_cache: HashMap::new(),
    };

    {
        EntityExample::insert_one(&mut connection, &entity).await?;
        EntityExample::insert_many(&mut connection, [&entity2]).await?;
        connection.append([&entity3]).await?;
    }

    {
        entity.save(&mut connection).await?;
        entity.delete(&mut connection).await?;
    }

    {
        // #region cheat-sheet-find
        use std::pin::pin;
        use tank::{Entity, expr, stream::TryStreamExt};

        let entity = EntityExample::find_one(&mut connection, entity.primary_key_expr()).await?;
        {
            let uid = entity2.unit_id;
            let mut stream = pin!(EntityExample::find_many(
                &mut connection,
                expr!(EntityExample::unit_id == #uid),
                Some(100),
            ));
            while let Some(entity) = stream.try_next().await? {
                println!("{}", entity.callsign);
            }
        }
        let uid = Uuid::parse_str("94f0cbcc-1fce-454e-a6e4-4e3587741808")?;
        let entities: Vec<EntityExample> =
            EntityExample::find_many(&mut connection, expr!(EntityExample::unit_id == #uid), None)
                .try_collect()
                .await?;
        // #endregion cheat-sheet-find
    }

    {
        use tank::{Entity, expr};

        let uid = entity2.unit_id;
        EntityExample::delete_many(&mut connection, expr!(EntityExample::unit_id == #uid)).await?;

        let uid = entity3.unit_id;
        EntityExample::delete_many(&mut connection, expr!(EntityExample::unit_id == #uid)).await?;
    }

    #[cfg(not(feature = "disable-transactions"))]
    {
        use tank::{Entity, Transaction};

        let mut tx = connection.begin().await?;
        EntityExample::insert_one(&mut tx, &entity).await?;
        entity.delete(&mut tx).await?;
        tx.commit().await?;
    }

    {
        expr!(EntityExample::casualties == 0);
        expr!(EntityExample::casualties >= 10);
        expr!(EntityExample::region == "North" || EntityExample::region == "South");
        expr!(EntityExample::callsign == "Alpha%" as LIKE);
        expr!(EntityExample::callsign != "Alpha%" as LIKE);
        expr!(EntityExample::casualties > ?);
        let uid = Uuid::new_v4();
        expr!(EntityExample::unit_id == #uid);
        let regions = ["North", "South"];
        expr!(EntityExample::region == #regions as IN);
        expr!(if EntityExample::casualties > 10 {
            "heavy"
        } else {
            "light"
        });
        expr!(match EntityExample::region {
            "North" | "South" => 1,
            _ => 2,
        });
    }

    {
        // #region cheat-sheet-prepared
        use tank::{
            Entity, expr,
            stream::{StreamExt, TryStreamExt},
        };

        EntityExample::insert_one(&mut connection, &entity).await?;
        let mut query = EntityExample::prepare_find(
            &mut connection,
            expr!(EntityExample::unit_id == ?),
            Some(50),
        )
        .await?;
        query.bind(entity.unit_id)?;
        let entities = connection
            .fetch(&mut query)
            .map(|row| row.and_then(EntityExample::from_row))
            .try_collect::<Vec<EntityExample>>()
            .await?;
        query.clear_bindings()?;
        query.bind(entity2.unit_id)?;
        // #endregion cheat-sheet-prepared

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].callsign, entity.callsign);
        assert!(entities[0].transient_cache.is_empty());
    }

    {
        // #region cheat-sheet-query-builder
        use tank::{
            Sql, cols, expr,
            stream::{StreamExt, TryStreamExt},
        };

        #[derive(Entity)]
        struct Slim {
            callsign: String,
            casualties: i32,
        }

        let uid = entity.unit_id;
        let results = connection
            .fetch(
                Sql()
                    .select(cols!(EntityExample::callsign, EntityExample::casualties))
                    .from(EntityExample::table())
                    .where_expr(expr!(EntityExample::unit_id == #uid))
                    .order_by(cols!(EntityExample::region ASC))
                    .limit(Some(50))
                    .build(&connection.driver()),
            )
            .map(|row| row.and_then(Slim::from_row))
            .try_collect::<Vec<_>>()
            .await?;
        // #endregion cheat-sheet-query-builder

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].callsign, entity.callsign);
        assert_eq!(results[0].casualties, entity.casualties);
    }

    #[cfg(not(feature = "disable-joins"))]
    {
        use crate::{Author, AuthorColumnTrait, Book, BookColumnTrait};
        // #region cheat-sheet-joins
        use tank::{Entity, Sql, cols, expr, join, stream::StreamExt, stream::TryStreamExt};

        #[derive(Entity)]
        struct BookWithAuthor {
            title: String,
            author: String,
        }

        let rows: Vec<BookWithAuthor> = connection
            .fetch(
                Sql()
                    .select(cols!(Book::title, Author::name as author))
                    .from(join!(Book JOIN Author ON Book::author == Author::id))
                    .where_expr(expr!(Book::year > 2000))
                    .order_by(cols!(Book::title ASC))
                    .build(&connection.driver()),
            )
            .map_ok(BookWithAuthor::from_row)
            .map(Result::flatten)
            .try_collect()
            .await?;

        let rows: Vec<BookWithAuthor> = connection
            .fetch(
                Sql()
                    .select(cols!(B.title, A.name as author))
                    .from(join!(Book B LEFT JOIN Author A ON B.author == A.author_id))
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .map_ok(BookWithAuthor::from_row)
            .map(Result::flatten)
            .try_collect()
            .await?;

        let dataset = join!(
            Book B
                LEFT JOIN Author A1 ON B.author == A1.author_id
                LEFT JOIN Author A2 ON B.co_author == A2.author_id
        );
        let _ = connection
            .fetch(
                Sql()
                    .select(cols!(B.title, A1.name as author, A2.name as co_author))
                    .from(dataset)
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
        // #endregion cheat-sheet-joins
    }

    {
        EntityExample::insert_many(&mut connection, [&entity2, &entity3]).await?;

        // #region cheat-sheet-streams
        use std::{future::ready, pin::pin};
        use tank::{AsValue, Entity, QueryResult, Row, Sql, cols, stream::TryStreamExt};

        // You can filter inside rust instead of the database
        let casualties: Vec<EntityExample> = connection
            .fetch(
                Sql()
                    .select(EntityExample::columns())
                    .from(EntityExample::table())
                    .build(&connection.driver()),
            )
            .and_then(|row| ready(EntityExample::from_row(row)))
            .try_filter(|entity| ready(entity.casualties > 0))
            .try_collect()
            .await?;
        let callsigns: Vec<&str> = casualties.iter().map(|v| v.callsign.as_str()).collect();

        // You can fold on a single column
        let total = connection
            .fetch(
                Sql()
                    .select(cols!(
                        SUM(EntityExample::casualties) as total,
                        EntityExample::region
                    ))
                    .from(EntityExample::table())
                    .group_by(EntityExample::region)
                    .order_by("total")
                    .limit(Some(1))
                    .build(&connection.driver()),
            )
            .map_ok(|row| i32::try_from_value(row.get_column(EntityExample::region)))
            .map(Result::flatten)
            .try_fold(0, |acc, cur| future::ready(Ok(acc + cur)))
            .await?;

        // You can process one value at a time, just remember to pin the stream
        let mut rows = Vec::new();
        {
            let mut stream = pin!(
                connection.run(
                    Sql()
                        .select(cols!(EntityExample::callsign))
                        .from(EntityExample::table())
                        .where_expr(true)
                        .build(&connection.driver()),
                )
            );
            while let Some(result) = stream.try_next().await? {
                match result {
                    // Rows borrow the connection until the stream is dropped, `into_owned` detaches them
                    QueryResult::Row(row) => rows.push(row.into_owned()),
                    QueryResult::Affected(affected) => println!("{:?}", affected.rows_affected),
                }
            }
        }
        // #endregion cheat-sheet-streams

        assert_eq!(callsigns.len(), 2);
        assert!(callsigns.contains(&entity2.callsign.as_str()));
        assert_eq!(total, 8);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| row.get_column("callsign").is_some()));
    }

    Ok(())
}
