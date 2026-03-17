use tank_core::{Connection, Executor, Transaction, Value, indoc::indoc, stream::TryStreamExt};

pub(crate) async fn structure(mut connection: impl Connection) {
    let mut tx = connection
        .begin()
        .await
        .expect("Failed to start a transaction");

    // users
    tx.execute(indoc! {"
                DROP TABLE IF EXISTS users;
                CREATE TABLE users (
                    id INTEGER,
                    info STRUCT (
                        name VARCHAR,
                        age INTEGER,
                        email VARCHAR
                    )
                );
                INSERT INTO users VALUES
                    (1, {name: 'Alice', age: 30, email: 'alice@example.com'}),
                    (2, {name: 'Bob', age: 25, email: 'bob@example.com'}),
                    (3, {name: 'Charlie', age: 35, email: 'charlie@example.com'});
            "})
        .await
        .expect("Could not start a transaction");
    tx.commit().await.expect("Faield to commit the transaction");
    let users = connection
        .fetch(indoc! {"
            SELECT id, info
            FROM users
            WHERE info.age >= 30
            ORDER BY id;
        "})
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query");
    assert_eq!(users.len(), 2);
    let Value::Struct(Some(alice), fields, table) = users[0].get_column("info").unwrap() else {
        panic!("Expected the field to be a struct containing some value");
    };

    assert_eq!(
        table.name, "",
        "TableRef name should be empty for anonymous struct"
    );
    assert_eq!(
        *fields,
        [
            ("name".to_string(), Value::Varchar(None)),
            ("age".to_string(), Value::Int32(None)),
            ("email".to_string(), Value::Varchar(None))
        ],
        "Struct fields definition mismatch"
    );

    assert_eq!(alice.len(), 3);
    assert_eq!(alice[0].0, "name");
    assert_eq!(alice[0].1, Value::Varchar(Some("Alice".into())));
    assert_eq!(alice[1].0, "age");
    assert_eq!(alice[1].1, Value::Int32(Some(30)));
    assert_eq!(alice[2].0, "email");
    assert_eq!(alice[2].1, Value::Varchar(Some("alice@example.com".into())));

    let Value::Struct(Some(charlie), ..) = users[1].get_column("info").unwrap() else {
        panic!("Expected struct for Charlie");
    };
    assert_eq!(charlie[0].1, Value::Varchar(Some("Charlie".into())));
    assert_eq!(charlie[1].1, Value::Int32(Some(35)));

    // data
    connection
        .execute(indoc! {"
        CREATE TABLE complex_items (
            id INTEGER,
            data STRUCT(
                tags VARCHAR[],
                meta STRUCT(
                    priority INTEGER,
                    label VARCHAR
                )
            )
        );
        INSERT INTO complex_items VALUES
            (100, {tags: ['urgent', 'work'], meta: {priority: 1, label: 'high'}});
    "})
        .await
        .expect("Failed to create complex table");

    let items = connection
        .fetch("SELECT data FROM complex_items WHERE id = 100")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query complex items");
    assert_eq!(items.len(), 1);
    let Value::Struct(Some(data), fields, _) = items[0].get_column("data").unwrap() else {
        panic!("Expected complex struct");
    };
    assert_eq!(fields[0].0, "tags");
    let tags_val = &data[0].1;
    match tags_val {
        Value::List(Some(list), _inner_type) => {
            assert_eq!(list.len(), 2);
            assert_eq!(list[0], Value::Varchar(Some("urgent".into())));
            assert_eq!(list[1], Value::Varchar(Some("work".into())));
        }
        Value::Array(Some(arr), _inner_type, _len) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Value::Varchar(Some("urgent".into())));
        }
        _ => panic!("Expected List or Array for tags, got {:?}", tags_val),
    }

    assert_eq!(fields[1].0, "meta");
    let meta_val = &data[1].1;
    if let Value::Struct(Some(meta_fields), _meta_type, _) = meta_val {
        assert_eq!(meta_fields[0].0, "priority");
        assert_eq!(meta_fields[0].1, Value::Int32(Some(1)));
        assert_eq!(meta_fields[1].0, "label");
        assert_eq!(meta_fields[1].1, Value::Varchar(Some("high".into())));
    } else {
        panic!("Expected nested struct for meta, got {:?}", meta_val);
    }

    // null_checks
    connection
        .execute(indoc! { "
            CREATE TABLE null_checks (
                id INTEGER,
                info STRUCT(
                    name VARCHAR,
                    score INTEGER
                )
            );
            INSERT INTO null_checks VALUES
                (1, NULL),
                (2, {'name': NULL, 'score': 3});
        "})
        .await
        .expect("Failed to create null_checks table");

    let null_rows = connection
        .fetch("SELECT id, info FROM null_checks ORDER BY id")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query null_checks");

    assert_eq!(null_rows.len(), 2);

    let info_col_1 = null_rows[0].get_column("info").unwrap();
    if let Value::Struct(None, fields, _) = info_col_1 {
        assert_eq!(
            fields.len(),
            2,
            "Should preserve schema even for NULL struct value"
        );
        assert_eq!(fields[0].0, "name");
        assert_eq!(fields[1].0, "score");
    } else {
        panic!(
            "Expected NULL struct for row 1 (Value::Struct(None, ..)), got {:?}",
            info_col_1
        );
    }

    let info_col_2 = null_rows[1].get_column("info").unwrap();
    if let Value::Struct(Some(values), _, _) = info_col_2 {
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].0, "name");
        assert_eq!(
            values[0].1,
            Value::Varchar(None),
            "Expected NULL Varchar for name"
        );
        assert_eq!(values[1].0, "score");
        assert_eq!(
            values[1].1,
            Value::Int32(Some(3)),
            "Expected Int32(3) for score"
        );
    } else {
        panic!(
            "Expected valid struct for row 2 (Value::Struct(Some(..), ..)), got {:?}",
            info_col_2
        );
    }
}
