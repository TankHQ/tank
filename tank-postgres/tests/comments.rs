#![allow(dead_code)]
use indoc::indoc;
use tank::{
    ConnectionPool, Driver, Entity, Executor, QueryBuilder, cols, expr,
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
struct PgColumn {
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    comment: Option<String>,
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

    let query = QueryBuilder::new()
        .select(cols!(
            PgColumn::table_schema,
            PgColumn::table_name,
            PgColumn::column_name,
            PgColumn::ordinal_position,
            col_description(
                to_regclass("comment_schema.commented_entity"),
                PgColumn::ordinal_position
            ) as comment
        ))
        .from(PgColumn::table())
        .where_expr(expr!(
            PgColumn::table_schema == "comment_schema"
                && PgColumn::table_name == "commented_entity"
                && col_description(
                    to_regclass("comment_schema.commented_entity"),
                    PgColumn::ordinal_position
                ) != NULL
        ))
        .order_by(cols!(PgColumn::ordinal_position ASC))
        .build(&connection.driver());
    let rows = connection
        .fetch(query)
        .map(|r| r.and_then(PgColumn::from_row))
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query Postgres column comments");
    assert_eq!(
        rows,
        [
            PgColumn {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "title".into(),
                ordinal_position: 2,
                comment: Some("Simple single-line comment".into()),
            },
            PgColumn {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "notes".into(),
                ordinal_position: 3,
                comment: indoc!(
                    r#"
                        Multi-line comment
                        It's got a quote and a second line

                        And a third after an empty line"
                    "#
                )
                .into(),
            },
        ]
    );
}
