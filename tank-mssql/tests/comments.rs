#![allow(dead_code)]
use indoc::indoc;
use tank::{
    ConnectionPool, Driver, Entity, Executor, QueryBuilder, cols, expr, join,
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

#[derive(Entity)]
#[tank(schema = "sys", name = "schemas")]
struct SysSchema {
    schema_id: i32,
    name: String,
}

#[derive(Entity)]
#[tank(schema = "sys", name = "tables")]
struct SysTable {
    object_id: i32,
    schema_id: i32,
    name: String,
}

#[derive(Entity)]
#[tank(schema = "sys", name = "columns")]
struct SysColumn {
    object_id: i32,
    column_id: i32,
    name: String,
}

#[derive(Entity)]
#[tank(schema = "sys", name = "extended_properties")]
struct SysExtendedProperty {
    major_id: i32,
    minor_id: i32,
    name: String,
    value: String,
}

#[derive(Entity, Debug, PartialEq)]
struct MSSQLColumnComment {
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
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

    let query = QueryBuilder::new()
        .select(cols!(
            SysSchema::name as table_schema,
            SysTable::name as table_name,
            SysColumn::name as column_name,
            SysColumn::column_id as ordinal_position,
            CAST(SysExtendedProperty::value as String) as comment,
        ))
        .from(join!(
            SysExtendedProperty
                JOIN SysTable ON SysExtendedProperty::major_id == SysTable::object_id
                JOIN SysSchema ON SysTable::schema_id == SysSchema::schema_id
                JOIN SysColumn ON SysExtendedProperty::major_id == SysColumn::object_id
                    && SysExtendedProperty::minor_id == SysColumn::column_id
        ))
        .where_expr(expr!(
            SysExtendedProperty::name == "MS_Description"
                && SysSchema::name == "comment_schema"
                && SysTable::name == "commented_entity"
        ))
        .order_by(cols!(SysColumn::column_id ASC))
        .build(&connection.driver());
    let rows = connection
        .fetch(query)
        .map(|r| r.and_then(MSSQLColumnComment::from_row))
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to query MSSQL column comments");
    assert_eq!(
        rows,
        [
            MSSQLColumnComment {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "title".into(),
                ordinal_position: 2,
                comment: "Simple single-line comment".into(),
            },
            MSSQLColumnComment {
                table_schema: "comment_schema".into(),
                table_name: "commented_entity".into(),
                column_name: "notes".into(),
                ordinal_position: 3,
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
