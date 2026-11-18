use std::{iter, sync::LazyLock};
use tank::{BoxQueryBuilder, Entity, Sql, expr};

#[derive(Entity, Debug, Clone, PartialEq)]
#[tank(name = "query_builder_crew")]
pub struct Crew {
    #[tank(primary_key)]
    pub id: i32,
    pub name: String,
    pub rank: i32,
}

/// The first three members are inserted first, the last two later.
pub static CREW: LazyLock<[Crew; 5]> = LazyLock::new(|| {
    [
        (1, "Alpha", 1),
        (2, "Bravo", 2),
        (3, "Charlie", 3),
        (4, "Delta", 4),
        (5, "Echo", 5),
    ]
    .map(|(id, name, rank)| Crew {
        id,
        name: name.into(),
        rank,
    })
});

/// Statements built without knowing the driver, rendered by whichever executor runs them.
///
/// Returns `[drop_table, create_table, insert, select, update, insert_more, delete]`.
pub fn crew_queries() -> [BoxQueryBuilder; 7] {
    [
        Sql().drop_table(Crew::table()).if_exists().boxed(),
        Sql().create_table(Crew::table()).if_not_exists().boxed(),
        Sql().insert_into(Crew::table()).values(&CREW[..3]).boxed(),
        Sql().select(Crew::columns()).from(Crew::table()).boxed(),
        Sql()
            .update(Crew::table())
            // Stored builders must own their expressions, `cols!` borrows temporaries
            .set(iter::once_with(|| expr!(Crew::rank == 10)))
            .where_expr(expr!(Crew::id == 2))
            .boxed(),
        Sql().insert_into(Crew::table()).values(&CREW[3..]).boxed(),
        Sql().delete_from(Crew::table()).where_expr(true).boxed(),
    ]
}
