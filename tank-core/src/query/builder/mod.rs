mod create_table;
mod drop_table;
mod insert_into;
mod select;

pub use create_table::*;
pub use drop_table::*;
pub use insert_into::*;
pub use select::*;

use crate::{Context, DynQuery, Entity, Expression, ExpressionMatcher, OpPrecedence, SqlWriter};
use std::iter;

#[derive(Default, Debug)]
pub struct QueryBuilder;

impl QueryBuilder {
    pub fn new() -> Self {
        Self {}
    }
    pub fn select<Select: ExpressionCollection>(
        self,
        select: Select,
    ) -> SelectQueryBuilder<Select, NA, NA, NA, NA, NA, NA> {
        SelectQueryBuilder {
            select,
            from: Default::default(),
            where_expr: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
    pub fn insert_into<E: Entity>(self) -> InsertIntoQueryBuilder<E, NA, NA> {
        InsertIntoQueryBuilder {
            values: Default::default(),
            update: Default::default(),
            _table: Default::default(),
            _update: Default::default(),
        }
    }
    pub fn create_table<E: Entity>(self) -> CreateTableQueryBuilder<E, NA> {
        CreateTableQueryBuilder {
            if_not_exists: Default::default(),
            _table: Default::default(),
            _e: Default::default(),
        }
    }
    pub fn drop_table<E: Entity>(self) -> DropTableQueryBuilder<E, NA> {
        DropTableQueryBuilder {
            if_exists: Default::default(),
            _table: Default::default(),
            _e: Default::default(),
        }
    }
}

#[derive(Default, Debug)]
pub struct NA;

impl OpPrecedence for NA {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        0
    }
}

impl Expression for NA {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut DynQuery) {}
    fn matches(&self, _matcher: &mut dyn ExpressionMatcher, _writer: &dyn SqlWriter) -> bool {
        false
    }
}

pub trait ExpressionCollection {
    fn expr_iter(&self) -> impl Iterator<Item = impl Expression> + Clone;
}

impl<I> ExpressionCollection for I
where
    Self: Clone,
    I: IntoIterator,
    <I as IntoIterator>::Item: Expression,
    <I as IntoIterator>::IntoIter: Clone,
{
    #[allow(refining_impl_trait)]
    fn expr_iter(&self) -> impl Iterator<Item = <I as IntoIterator>::Item> + Clone {
        self.clone().into_iter()
    }
}

impl ExpressionCollection for NA {
    fn expr_iter(&self) -> impl Iterator<Item = impl Expression> + Clone {
        iter::empty::<&dyn Expression>()
    }
}
