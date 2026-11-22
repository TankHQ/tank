#![allow(unused_variables)]
use crate::silent_logs;
use std::sync::LazyLock;
use tokio::sync::Mutex;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

// #region cheat-sheet-conversion
use anyhow::anyhow;

#[derive(Clone)]
pub struct Notes(pub String); // Third party type (cannot impl tank::AsValue)
pub struct NotesWrap(pub Notes); // Wrapper type defined in this crate

impl tank::AsValue for NotesWrap {
    fn as_empty_value() -> tank::ValueOwned {
        tank::Value::Varchar(None) // You decide the value it translates to
    }
    fn as_value<'a>(self) -> tank::Value<'a>
    where
        Self: 'a,
    {
        tank::Value::Varchar(Some(self.0.0.into()))
    }
    fn decode_value(value: impl tank::AsValue) -> tank::Result<Self> {
        // Take advantage of the tank::Value conversion.
        // Call `value.as_value().try_as(&Self::as_empty_value())` first.
        match value.as_value().try_as(&Self::as_empty_value()) {
            // Once you have the specific `tank::Value` variant, convert it back.
            Ok(tank::Value::Varchar(Some(s))) => Ok(Self(Notes(s.to_string()))),
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
use std::collections::HashMap;
use tank::Entity;
use uuid::Uuid;

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

pub async fn cheat_sheet(mut connection: &mut impl tank::Connection) -> tank::Result<()> {
    let _lock = MUTEX.lock().await;

    EntityExample::drop_table(&mut connection, true, false).await?;
    EntityExample::create_table(&mut connection, true, true).await?;

    let entity = EntityExample {
        unit_id: Uuid::new_v4(),
        region: "North".into(),
        callsign: "Alpha".into(),
        casualties: 0,
        metadata: Notes("mission 1".into()),
        transient_cache: HashMap::new(),
    };
    let entity2 = EntityExample {
        unit_id: Uuid::new_v4(),
        region: "South".into(),
        callsign: "Bravo-2".into(),
        casualties: 3,
        metadata: Notes("mission 2".into()),
        transient_cache: HashMap::new(),
    };
    let entity3 = EntityExample {
        unit_id: uuid::uuid!("94f0cbcc-1fce-454e-a6e4-4e3587741808"),
        region: "East".into(),
        callsign: "Charlie-3".into(),
        casualties: 5,
        metadata: Notes("mission 3".into()),
        transient_cache: HashMap::new(),
    };

    {
        // #region cheat-sheet-insert
        EntityExample::insert_one(&mut connection, &entity).await?;
        EntityExample::insert_many(&mut connection, [&entity2]).await?;
        // #endregion cheat-sheet-insert

        assert!(
            EntityExample::find_one(&mut connection, entity.primary_key_expr())
                .await?
                .is_some()
        );
    }

    {
        // #region cheat-sheet-save-delete
        entity.save(&mut connection).await?;
        entity.delete(&mut connection).await?;
        // #endregion cheat-sheet-save-delete

        assert!(
            EntityExample::find_one(&mut connection, entity.primary_key_expr())
                .await?
                .is_none()
        );
    }

    {
        // #region cheat-sheet-find
        use std::pin::pin;
        use tank::{expr, stream::TryStreamExt};
        use uuid::uuid;

        let found = EntityExample::find_one(&mut connection, entity2.primary_key_expr()).await?;
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
        let uid = uuid!("94f0cbcc-1fce-454e-a6e4-4e3587741808");
        let entities: Vec<EntityExample> =
            EntityExample::find_many(&mut connection, expr!(EntityExample::unit_id == #uid), None)
                .try_collect()
                .await?;
        // #endregion cheat-sheet-find

        assert_eq!(found.map(|e| e.callsign), Some(entity2.callsign.clone()));
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].callsign, entity3.callsign);
    }

    {
        // #region cheat-sheet-delete-many
        use tank::expr;

        let uid = entity2.unit_id;
        EntityExample::delete_many(&mut connection, expr!(EntityExample::unit_id == #uid)).await?;

        let uid = entity3.unit_id;
        EntityExample::delete_many(&mut connection, expr!(EntityExample::unit_id == #uid)).await?;
        // #endregion cheat-sheet-delete-many

        assert!(
            EntityExample::find_one(&mut connection, entity2.primary_key_expr())
                .await?
                .is_none()
        );
        assert!(
            EntityExample::find_one(&mut connection, entity3.primary_key_expr())
                .await?
                .is_none()
        );
    }

    #[cfg(not(feature = "disable-transactions"))]
    {
        // #region cheat-sheet-transaction
        use tank::Transaction;

        let mut tx = connection.begin().await?;
        EntityExample::insert_one(&mut tx, &entity).await?;
        entity.delete(&mut tx).await?;
        tx.commit().await?; // Or tx.rollback().await?
        // #endregion cheat-sheet-transaction

        assert!(
            EntityExample::find_one(&mut connection, entity.primary_key_expr())
                .await?
                .is_none()
        );
    }

    {
        // #region cheat-sheet-expressions
        use tank::expr;
        use uuid::Uuid;

        expr!(EntityExample::casualties == 0);
        expr!(EntityExample::casualties >= 10);
        expr!(EntityExample::region == "North" || EntityExample::region == "South");
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
        // #endregion cheat-sheet-expressions
    }

    {
        // #region cheat-sheet-prepared
        use tank::{
            expr,
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
        // Fetch again
        // #endregion cheat-sheet-prepared

        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].callsign, entity.callsign);
        assert!(entities[0].transient_cache.is_empty());
    }

    {
        // #region cheat-sheet-query-builder
        use tank::{
            cols, expr,
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
                tank::Sql()
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
        use tank::{
            cols, expr, join,
            stream::{StreamExt, TryStreamExt},
        };

        #[derive(Entity)]
        struct BookWithAuthor {
            title: String,
            author: String,
        }

        let rows: Vec<BookWithAuthor> = connection
            .fetch(
                tank::Sql()
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
                tank::Sql()
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
                tank::Sql()
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
        use tank::{Row, cols, stream::TryStreamExt};

        // You can filter inside rust instead of the database
        let casualties: Vec<EntityExample> = connection
            .fetch(
                tank::Sql()
                    .select(EntityExample::columns())
                    .from(EntityExample::table())
                    .build(&connection.driver()),
            )
            .and_then(|row| ready(EntityExample::from_row(row)))
            .try_filter(|entity| ready(entity.casualties > 0))
            .try_collect()
            .await?;
        let callsigns: Vec<&str> = casualties.iter().map(|v| v.callsign.as_str()).collect();

        // You can fold on a single column, read by name
        #[cfg(not(feature = "disable-groups"))]
        let total = {
            use tank::{AsValue, stream::StreamExt};
            connection
                .fetch(
                    tank::Sql()
                        .select(cols!(
                            SUM(EntityExample::casualties) as total,
                            EntityExample::region
                        ))
                        .from(EntityExample::table())
                        .group_by(EntityExample::region)
                        .build(&connection.driver()),
                )
                .map_ok(|row| i32::decode_value(row.get_column("total")))
                .map(Result::flatten)
                .try_fold(0, |total, casualties| ready(Ok(total + casualties)))
                .await?
        };

        // You can process one value at a time, just remember to pin the stream
        let mut rows = Vec::new();
        {
            let mut stream = pin!(
                connection.run(
                    tank::Sql()
                        .select(cols!(EntityExample::callsign))
                        .from(EntityExample::table())
                        .where_expr(true)
                        .build(&connection.driver()),
                )
            );
            while let Some(result) = stream.try_next().await? {
                match result {
                    // Rows borrow the connection until the stream is dropped, `into_owned` detaches them
                    tank::QueryResult::Row(row) => rows.push(row.into_owned()),
                    tank::QueryResult::Affected(affected) => {
                        println!("{:?}", affected.rows_affected)
                    }
                }
            }
        }
        // #endregion cheat-sheet-streams

        assert_eq!(callsigns.len(), 2);
        assert!(callsigns.contains(&entity2.callsign.as_str()));
        #[cfg(not(feature = "disable-groups"))]
        assert_eq!(total, 8);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| row.get_column("callsign").is_some()));
    }

    {
        // #region cheat-sheet-builder-insert
        connection
            .execute(
                tank::Sql()
                    .insert_into(EntityExample::table())
                    .values([&entity])
                    .update() // Upsert on the primary key
                    .build(&connection.driver()),
            )
            .await?;
        // #endregion cheat-sheet-builder-insert
    }

    #[cfg(not(feature = "disable-updates"))]
    {
        // #region cheat-sheet-builder-update
        use tank::cols;

        connection
            .execute(
                tank::Sql()
                    .update(EntityExample::table())
                    .set(cols!(EntityExample::casualties == 1))
                    .where_expr(entity.primary_key_expr())
                    .build(&connection.driver()),
            )
            .await?;
        // #endregion cheat-sheet-builder-update

        let updated = EntityExample::find_one(&mut connection, entity.primary_key_expr())
            .await?
            .expect("Updated entity should exist");
        assert_eq!(updated.casualties, 1);
    }

    {
        // #region cheat-sheet-builder-delete
        connection
            .execute(
                tank::Sql()
                    .delete_from(EntityExample::table())
                    .where_expr(entity3.primary_key_expr())
                    .build(&connection.driver()),
            )
            .await?;
        // #endregion cheat-sheet-builder-delete

        assert!(
            EntityExample::find_one(&mut connection, entity3.primary_key_expr())
                .await?
                .is_none()
        );
    }

    {
        // #region cheat-sheet-builder-create-table
        connection
            .execute(
                tank::Sql()
                    .create_table(("army", "outposts"))
                    .if_not_exists()
                    .columns([
                        tank::ColumnDef::from_type::<i32>("id").primary_key(),
                        tank::ColumnDef::from_type::<String>("sector").nullable(true),
                    ])
                    .build(&connection.driver()),
            )
            .await?;
        // #endregion cheat-sheet-builder-create-table
    }

    silent_logs! {
        // #region cheat-sheet-builder-drop-table
        connection
            .execute(
                tank::Sql()
                    .drop_table(("army", "outposts"))
                    .if_exists()
                    .build(&connection.driver()),
            )
            .await?;
        // #endregion cheat-sheet-builder-drop-table
    }

    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        EntityExample::drop_table(&mut connection, true, false).await?;

        // #region cheat-sheet-sql-writer
        use tank::{Driver, SqlWriter, stream::TryStreamExt};

        let writer = connection.driver().sql_writer();
        let mut query = tank::AnyQuery::default();

        writer.write_create_table(&mut query, EntityExample::table(), true);
        writer.write_insert(
            &mut query,
            tank::Sql()
                .insert_into(EntityExample::table())
                .values([&entity, &entity2]),
        );
        writer.write_select(
            &mut query,
            tank::Sql()
                .select(EntityExample::columns())
                .from(EntityExample::table())
                .where_expr(true)
                .limit(Some(100)),
        );

        let results: Vec<_> = connection.run(query).try_collect().await?;
        // #endregion cheat-sheet-sql-writer

        let row_count = results
            .iter()
            .filter(|r| matches!(r, tank::QueryResult::Row(_)))
            .count();
        assert_eq!(row_count, 2);
    }

    {
        // #region cheat-sheet-table-setup
        EntityExample::create_table(&mut connection, true, true).await?;
        EntityExample::drop_table(&mut connection, true, false).await?;
        // #endregion cheat-sheet-table-setup
    }

    Ok(())
}
