#![allow(unused_imports)]
use std::{collections::HashSet, pin::pin, sync::LazyLock};
use tank::{
    DynQuery, AsValue, DataSet, Driver, Entity, Executor, Passive, Query, QueryBuilder, QueryResult,
    RowLabeled, SqlWriter, Value, cols, expr, join, stream::{StreamExt, TryStreamExt}
};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(schema = "testing", name = "authors")]
pub struct Author {
    #[tank(primary_key, name = "author_id")]
    pub id: Passive<Uuid>,
    pub name: String,
    pub country: String,
    pub books_published: Option<u16>,
}

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(schema = "testing", name = "books", primary_key = (Self::title, Self::author))]
pub struct Book {
    #[cfg(not(feature = "disable-arrays"))]
    pub isbn: [u8; 13],
    #[tank(column_type = (mysql = "VARCHAR(255)"))]
    pub title: String,
    /// Main author
    #[tank(references = Author::id)]
    pub author: Uuid,
    #[tank(references = Author::id)]
    pub co_author: Option<Uuid>,
    pub year: i32,
}

pub async fn books<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Book::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Book table");
    Author::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Author table");
    Author::create_table(executor, false, true)
        .await
        .expect("Failed to create Author table");
    Book::create_table(executor, false, true)
        .await
        .expect("Failed to create Book table");

    // Author objects
    let authors = vec![
        Author {
            id: Uuid::parse_str("f938f818-0a40-4ce3-8fbc-259ac252a1b5")
                .unwrap()
                .into(),
            name: "J.K. Rowling".into(),
            country: "UK".into(),
            books_published: 24.into(),
        },
        Author {
            id: Uuid::parse_str("a73bc06a-ff89-44b9-a62f-416ebe976285")
                .unwrap()
                .into(),
            name: "J.R.R. Tolkien".into(),
            country: "USA".into(),
            books_published: 6.into(),
        },
        Author {
            id: Uuid::parse_str("6b2f56a1-316d-42b9-a8ba-baca42c5416c")
                .unwrap()
                .into(),
            name: "Dmitrij Gluchovskij".into(),
            country: "Russia".into(),
            books_published: 7.into(),
        },
        Author {
            id: Uuid::parse_str("d3d3d3d3-d3d3-d3d3-d3d3-d3d3d3d3d3d3")
                .unwrap()
                .into(),
            name: "Linus Torvalds".into(),
            country: "Finland".into(),
            books_published: None,
        },
    ];
    let rowling_id = authors[0].id.clone().unwrap();
    let tolkien_id = authors[1].id.clone().unwrap();
    let gluchovskij_id = authors[2].id.clone().unwrap();

    // Book objects
    let books = vec![
        Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 0, 7, 4, 7, 5, 3, 2, 6, 9, 9],
            title: "Harry Potter and the Philosopher's Stone".into(),
            author: rowling_id,
            co_author: None,
            year: 1937,
        },
        Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 0, 7, 4, 7, 5, 9, 1, 0, 5, 4],
            title: "Harry Potter and the Deathly Hallows".into(),
            author: rowling_id,
            co_author: None,
            year: 2007,
        },
        Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 0, 6, 1, 8, 2, 6, 0, 3, 0, 0],
            title: "The Hobbit".into(),
            author: tolkien_id,
            co_author: None,
            year: 1996,
        },
        Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 5, 1, 7, 0, 5, 9, 6, 7, 8, 2],
            title: "Metro 2033".into(),
            author: gluchovskij_id,
            co_author: None,
            year: 2002,
        },
        Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 0, 0, 2, 3, 4, 5, 6, 7, 8, 9],
            title: "Hogwarts 2033".into(),
            author: rowling_id,
            co_author: gluchovskij_id.into(),
            year: 2026,
        },
    ];

    // Insert
    let result = Author::insert_many(executor, authors.iter())
        .await
        .expect("Failed to insert authors");
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 4);
    }
    let result = Book::insert_many(executor, books.iter())
        .await
        .expect("Failed to insert books");
    if let Some(affected) = result.rows_affected {
        assert_eq!(affected, 5);
    }

    // Find authors
    let id = Uuid::parse_str("f938f818-0a40-4ce3-8fbc-259ac252a1b5").unwrap().as_value();
    let author = Author::find_one(
        executor,
        expr!(Author::id == #id)
    )
        .await
        .expect("Failed to query author by pk");
    assert_eq!(
        author,
        Some(Author {
            id: Uuid::parse_str("f938f818-0a40-4ce3-8fbc-259ac252a1b5")
                .unwrap()
                .into(),
            name: "J.K. Rowling".into(),
            country: "UK".into(),
            books_published: 24.into(),
        })
    );

    let author = Author::find_one(executor, expr!(Author::name == "Linus Torvalds"))
        .await
        .expect("Failed to query author by pk");
    assert_eq!(
        author,
        Some(Author {
            id: Uuid::parse_str("d3d3d3d3-d3d3-d3d3-d3d3-d3d3d3d3d3d3")
                .unwrap()
                .into(),
            name: "Linus Torvalds".into(),
            country: "Finland".into(),
            books_published: None,
        })
    );

    // Get books before 2000
    let result = executor
        .fetch(
            QueryBuilder::new()
                .select(&[expr!(B.title), expr!(A.name)])
                .from(join!(Book B JOIN Author A ON B.author == A.author_id))
                .where_condition(expr!(B.year < 2000))
                .build(&executor.driver()),
        )
        .try_collect::<Vec<RowLabeled>>()
        .await
        .expect("Failed to query books and authors joined")
        .into_iter()
        .map(|row| {
            let mut iter = row.values.into_iter();
            (
                match iter.next().unwrap() {
                    Value::Varchar(Some(v)) => v,
                    Value::Unknown(Some(v)) => v.into(),
                    v => panic!("Expected first value to be non null varchar, found {v:?}"),
                },
                match iter.next().unwrap() {
                    Value::Varchar(Some(v)) => v,
                    Value::Unknown(Some(v)) => v.into(),
                    v => panic!("Expected second value to be non null varchar, found {v:?}"),
                },
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        result,
        HashSet::from_iter([
            (
                "Harry Potter and the Philosopher's Stone".into(),
                "J.K. Rowling".into()
            ),
            ("The Hobbit".into(), "J.R.R. Tolkien".into()),
        ])
    );

    // Get all books with their authors
    let dataset = join!(
        Book B LEFT JOIN Author A1 ON B.author == A1.author_id
            LEFT JOIN Author A2 ON B.co_author == A2.author_id
    );
    let result = executor.fetch(
            QueryBuilder::new()
                .select(cols!(B.title, A1.name as author, A2.name as co_author))
                .from(dataset)
                .where_condition(true)
                .build(&executor.driver())
        ) 
        .try_collect::<Vec<RowLabeled>>()
        .await
        .expect("Failed to query books and authors joined")
        .into_iter()
        .map(|row| {
            let mut iter = row.values.into_iter();
            (
                match iter.next().unwrap() {
                    Value::Varchar(Some(v)) => v,
                    Value::Unknown(Some(v)) => v.into(),
                    v => panic!("Expected 1st value to be non null varchar, found {v:?}"),
                },
                match iter.next().unwrap() {
                    Value::Varchar(Some(v)) => v,
                    Value::Unknown(Some(v)) => v.into(),
                    v => panic!("Expected 2nd value to be non null varchar, found {v:?}"),
                },
                match iter.next().unwrap() {
                    Value::Varchar(Some(v)) => Some(v),
                    Value::Unknown(Some(v)) => Some(v.into()),
                    Value::Varchar(None) | Value::Null => None,
                    v => panic!(
                        "Expected 3rd value to be a Some(Value::Varchar(..)) | Value::Unknown(Some(..)) | Some(Value::Null)), found {v:?}",
                    ),
                },
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        result,
        HashSet::from_iter([
            (
                "Harry Potter and the Philosopher's Stone".into(),
                "J.K. Rowling".into(),
                None
            ),
            (
                "Harry Potter and the Deathly Hallows".into(),
                "J.K. Rowling".into(),
                None
            ),
            ("The Hobbit".into(), "J.R.R. Tolkien".into(), None),
            ("Metro 2033".into(), "Dmitrij Gluchovskij".into(), None),
            (
                "Hogwarts 2033".into(),
                "J.K. Rowling".into(),
                Some("Dmitrij Gluchovskij".into())
            ),
        ])
    );

    // Get book and author pairs
    #[derive(Debug, Entity, PartialEq, Eq, Hash)]
    struct Books {
        pub title: Option<String>,
        pub author: Option<String>,
    }
    let books = executor.fetch(
            QueryBuilder::new()
                .select(cols!(Book::title, Author::name as author, Book::year))
                .from(join!(Book JOIN Author ON Book::author == Author::id))
                .where_condition(true)
                .build(&executor.driver())
        )
        .and_then(|row| async { Books::from_row(row) })
        .try_collect::<HashSet<_>>()
        .await
        .expect("Could not return the books");
    assert_eq!(
        books,
        HashSet::from_iter([
            Books {
                title: Some("Harry Potter and the Philosopher's Stone".into()),
                author: Some("J.K. Rowling".into())
            },
            Books {
                title: Some("Harry Potter and the Deathly Hallows".into()),
                author: Some("J.K. Rowling".into())
            },
            Books {
                title: Some("The Hobbit".into()),
                author: Some("J.R.R. Tolkien".into())
            },
            Books {
                title: Some("Metro 2033".into()),
                author: Some("Dmitrij Gluchovskij".into())
            },
            Books {
                title: Some("Hogwarts 2033".into()),
                author: Some("J.K. Rowling".into())
            },
        ])
    );

    #[cfg(not(feature = "disable-references"))]
    {
        // Insert book violating referential integrity
        use crate::silent_logs;
        let book = Book {
            #[cfg(not(feature = "disable-arrays"))]
            isbn: [9, 7, 8, 1, 7, 3, 3, 5, 6, 1, 0, 8, 0],
            title: "My book".into(),
            author: Uuid::parse_str("c18c04b4-1aae-48a3-9814-9b70f7a38315").unwrap(),
            co_author: None,
            year: 2025,
        };
        silent_logs! {
            assert!(
                book.save(executor).await.is_err(),
                "Must fail to save book violating referential integrity"
            );
        }
    }

    #[cfg(not(feature = "disable-ordering"))]
    {
        // Authors names alphabetical order
        let authors = executor.fetch(
            QueryBuilder::new()
                .select(cols!(Author::name ASC))
                .from(Author::table())
                .where_condition(true)
                .build(&executor.driver())
            )
            .and_then(|row| async move { AsValue::try_from_value((*row.values)[0].clone()) })
            .try_collect::<Vec<String>>()
            .await
            .expect("Could not return the ordered names of the authors");
        assert_eq!(
            authors,
            vec![
                "Dmitrij Gluchovskij".to_string(),
                "J.K. Rowling".to_string(),
                "J.R.R. Tolkien".to_string(),
                "Linus Torvalds".to_string(),
            ]
        )
    }

    // Multiple statements
    #[cfg(not(feature = "disable-multiple-statements"))]
    {
        let mut query = DynQuery::default();
        let writer = executor.driver().sql_writer();
        writer.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(Book::columns())
                .from(Book::table())
                .where_condition(expr!(Book::title == "Metro 2033"))
                .limit(Some(1))
        );
        writer.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(Book::columns())
                .from(Book::table())
                .where_condition(expr!(Book::title == "Harry Potter and the Deathly Hallows"))
                .limit(Some(1))
        );
        let mut stream = pin!(executor.run(query));
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the first row")
        };
        let book = Book::from_row(row).expect("Could not get the book from row");
        assert_eq!(
            book,
            Book {
                #[cfg(not(feature = "disable-arrays"))]
                isbn: [9, 7, 8, 5, 1, 7, 0, 5, 9, 6, 7, 8, 2],
                title: "Metro 2033".into(),
                author: gluchovskij_id,
                co_author: None,
                year: 2002,
            }
        );
        let Some(Ok(QueryResult::Row(row))) = stream.next().await else {
            panic!("Could not get the second row")
        };
        let book = Book::from_row(row).expect("Could not get the book from row");
        assert_eq!(
            book,
            Book {
                #[cfg(not(feature = "disable-arrays"))]
                isbn: [9, 7, 8, 0, 7, 4, 7, 5, 9, 1, 0, 5, 4],
                title: "Harry Potter and the Deathly Hallows".into(),
                author: rowling_id,
                co_author: None,
                year: 2007,
            }
        );
        assert!(
            stream.next().await.is_none(),
            "The stream should return only two rows"
        )
    }
}
