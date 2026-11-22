#![allow(dead_code)]
use indoc::indoc;
use tank::{
    ConnectionPool, Driver, Entity, Executor, Sql, cols, expr,
    stream::{StreamExt, TryStreamExt},
};

#[derive(Entity)]
#[tank(schema = "comment_schema", name = "commented_entity")]
struct CommentedEntity {
    #[tank(primary_key)]
    id: i32,
    /// Simple single-line comment
    title: String,
    /// Multi-line comment
    /// It's got a quote and a second line
    ///
    /// And a third after an empty line
    notes: Option<String>,
    uncommented: i64,
}

#[derive(Entity, Debug, PartialEq)]
#[tank(schema = "information_schema", name = "columns")]
struct MySQLColumn {
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: u64,
    column_comment: String,
}

pub(crate) async fn comments<D: Driver>(pool: &mut impl ConnectionPool<D>) {
    let mut connection = pool
        .get()
        .await
        .expect("Could not get a connection from the pool");
    CommentedEntity::drop_table(&mut connection, true, false)
        .await
        .expect("Failed to drop CommentedEntity table");
    CommentedEntity::create_table(&mut connection, true, true)
        .await
        .expect("Failed to create CommentedEntity table");

    let query = Sql()
        .select(cols!(
            MySQLColumn::table_schema as table_schema,
            MySQLColumn::table_name as table_name,
            MySQLColumn::column_name as column_name,
            MySQLColumn::ordinal_position as ordinal_position,
            MySQLColumn::column_comment as column_comment,
        ))
        .from(MySQLColumn::table())
        .where_expr(expr!(
            MySQLColumn::table_schema == "comment_schema"
                && MySQLColumn::table_name == "commented_entity"
                && MySQLColumn::column_comment != ""
        ))
        .order_by(cols!(MySQLColumn::ordinal_position ASC))
        .build(&connection.driver());
    let rows = connection
        .fetch(query)
        .map(|r| r.and_then(MySQLColumn::from_row))
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query MySQL/MariaDB column comments");
    assert_eq!(
        rows,
        [
            MySQLColumn {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "title".into(),
                ordinal_position: 2,
                column_comment: "Simple single-line comment".into(),
            },
            MySQLColumn {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "notes".into(),
                ordinal_position: 3,
                column_comment: indoc! {r#"
                    Multi-line comment
                    It's got a quote and a second line

                    And a third after an empty line
                "#}
                .trim()
                .into(),
            },
        ]
    );
}
