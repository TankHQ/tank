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
#[tank(name = "duckdb_columns")]
struct DuckDBColumn {
    schema_name: String,
    table_name: String,
    column_name: String,
    column_index: i64,
    comment: String,
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

    let query = Sql
        .select(DuckDBColumn::columns())
        .from(DuckDBColumn::table())
        .where_expr(expr!(
            DuckDBColumn::schema_name == "comment_schema"
                && DuckDBColumn::table_name == "commented_entity"
                && DuckDBColumn::comment != NULL
        ))
        .order_by(cols!(DuckDBColumn::column_index ASC))
        .build(&connection.driver());
    let rows = connection
        .fetch(query)
        .map(|r| r.and_then(DuckDBColumn::from_row))
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query duckdb_columns");
    assert_eq!(
        rows,
        [
            DuckDBColumn {
                schema_name: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "title".into(),
                column_index: 2,
                comment: "Simple single-line comment".into(),
            },
            DuckDBColumn {
                schema_name: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "notes".into(),
                column_index: 3,
                comment: indoc!(
                    r#"
                        Multi-line comment
                        It's got a quote and a second line

                        And a third after an empty line
                    "#
                )
                .trim()
                .into(),
            },
        ]
    );
}
