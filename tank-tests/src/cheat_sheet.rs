use anyhow::anyhow;
use std::{collections::HashMap, str::FromStr, sync::LazyLock};
use tank::{AsValue, Connection, Entity, Result, Transaction, Value, expr};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, PartialEq, Clone)]
pub struct Notes(pub String); // Third party type
pub struct NotesWrap(pub Notes); // Local wrapper

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

pub async fn cheat_sheet(mut connection: &mut impl Connection) -> Result<()> {
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

    {
        let mut tx = connection.begin().await?;
        entity.save(&mut tx).await?;
        EntityExample::delete_many(&mut tx, expr!(true)).await?;
        tx.commit().await?;
    }

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
        use std::pin::pin;
        use tank::{Entity, expr, stream::TryStreamExt};

        let entity = EntityExample::find_one(&mut connection, entity.primary_key_expr()).await?;
        {
            let mut stream = pin!(EntityExample::find_many(
                &mut connection,
                expr!(EntityExample::casualties > 0),
                Some(100),
            ));
            while let Some(entity) = stream.try_next().await? {
                println!("{}", entity.callsign);
            }
        }
        let uid = Uuid::from_str("94f0cbcc-1fce-454e-a6e4-4e3587741808")?;
        let entities: Vec<EntityExample> =
            EntityExample::find_many(&mut connection, expr!(EntityExample::unit_id == #uid), None)
                .try_collect()
                .await?;
    }

    {
        use tank::{Entity, expr};
        use uuid::Uuid;

        EntityExample::delete_many(&mut connection, expr!(EntityExample::casualties == 0)).await?;

        let uid = Uuid::from_str("2ed19568-a1ed-423d-aa81-735e75bb6b14").unwrap();
        EntityExample::delete_many(&mut connection, expr!(EntityExample::unit_id == #uid)).await?;
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
    }

    {
        use tank::{Entity, expr, stream::TryStreamExt};

        let mut query = EntityExample::prepare_find(
            &mut connection,
            expr!(EntityExample::unit_id == ?),
            Some(50),
        )
        .await?;
        query.bind(736621)?;
        let entities = connection
            .fetch(&mut query)
            .map_ok(|row| EntityExample::from_row(row).unwrap())
            .try_collect::<Vec<EntityExample>>()
            .await?;
        query.clear_bindings()?;
        query.bind(88221)?;
    }

    {
        use tank::{QueryBuilder, cols, expr, stream::TryStreamExt};

        let results = connection
            .fetch(
                QueryBuilder::new()
                    .select(cols!(EntityExample::callsign, EntityExample::casualties))
                    .from(EntityExample::table())
                    .where_expr(expr!(EntityExample::casualties > 0))
                    .order_by(cols!(EntityExample::casualties DESC))
                    .limit(Some(50))
                    .build(&connection.driver()),
            )
            .map_ok(|row| EntityExample::from_row(row).unwrap())
            .try_collect::<Vec<_>>()
            .await?;
    }

    #[cfg(not(feature = "disable-joins"))]
    {
        use tank::{Entity, QueryBuilder, cols, expr, join, stream::TryStreamExt};

        #[derive(Entity, Debug)]
        struct BookWithAuthor {
            title: String,
            author: String,
        }

        let rows: Vec<BookWithAuthor> = connection
            .fetch(
                QueryBuilder::new()
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
                QueryBuilder::new()
                    .select(cols!(B.title, A.name as author))
                    .from(join!(Book B LEFT JOIN Author A ON B.author_id == A.id))
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .map_ok(BookWithAuthor::from_row)
            .map(Result::flatten)
            .try_collect()
            .await?;

        let dataset = join!(
            Book B
                LEFT JOIN Author A1 ON B.author_id == A1.id
                LEFT JOIN Author A2 ON B.co_author_id == A2.id
        );
        let rows = connection
            .fetch(
                QueryBuilder::new()
                    .select(cols!(B.title, A1.name as author, A2.name as co_author))
                    .from(dataset)
                    .where_expr(true)
                    .build(&connection.driver()),
            )
            .try_collect::<Vec<_>>()
            .await?;
    }

    {
        use indoc::indoc;
        use std::pin::pin;
        use tank::{QueryResult, stream::TryStreamExt};

        {
            let mut stream = pin!(connection.run(indoc! {r#"
            SELECT unit_id, callsign
            FROM army.deployments
            WHERE casualties > 0
        "#}));
            while let Some(result) = stream.try_next().await? {
                match result {
                    QueryResult::Row(row) => {
                        println!("{:?}", row.values);
                    }
                    QueryResult::Affected(v) => {
                        println!("affected: {:?}", v.rows_affected);
                    }
                }
            }
        }
        let rows: Vec<_> = connection
            .fetch("SELECT * FROM army.deployments")
            .try_collect()
            .await?;
        let affected = connection
            .execute(indoc! {r#"
                UPDATE army.deployments SET casualties = 0
                WHERE region = 'North'
            "#})
            .await?;
    }

    {
        use anyhow::anyhow;
        use indoc::indoc;
        use std::pin::pin;
        use tank::{Entity, Row, stream::TryStreamExt};

        let mut query = connection
            .prepare(indoc! {r#"
                    SELECT unit_id, callsign
                    FROM army.deployments
                    WHERE unit_id = ?
                    LIMIT ?
                "#})
            .await?;
        query.bind_index(57383, 0)?;
        query.bind_index(1, 1)?;

        let row: Row = pin!(connection.fetch(&mut query))
            .try_next()
            .await?
            .ok_or(anyhow!("Not found"))?;
        let entity = EntityExample::from_row(row.clone())?;

        #[derive(Entity)]
        struct Slim {
            callsign: String,
            casualties: i32,
        }
        let slim = Slim::from_row(row)?;
        query.clear_bindings()?;
        query.bind(34724)?;
        query.bind(1)?;
    }

    Ok(())
}
