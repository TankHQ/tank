mod create_schema;
mod create_table;
mod drop_schema;
mod drop_table;
mod select;

pub use create_schema::*;
pub use create_table::*;
pub use drop_schema::*;
pub use drop_table::*;
pub use select::*;

use crate::{Context, DynQuery, Entity, Expression, ExpressionVisitor, OpPrecedence, SqlWriter};
use std::{borrow::Cow, iter};

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
    pub fn create_table<E: Entity>(self) -> CreateTableQueryBuilder<E> {
        CreateTableQueryBuilder {
            if_not_exists: Default::default(),
            _table: Default::default(),
        }
    }
    pub fn drop_table<E: Entity>(self) -> DropTableQueryBuilder<E> {
        DropTableQueryBuilder {
            if_exists: Default::default(),
            _table: Default::default(),
        }
    }
    pub fn create_schema(self, schema: Cow<'static, str>) -> CreateSchemaQueryBuilder {
        CreateSchemaQueryBuilder::new(schema)
    }
    pub fn drop_schema(self, schema: Cow<'static, str>) -> DropSchemaQueryBuilder {
        DropSchemaQueryBuilder::new(schema)
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
    fn accept_visitor(
        &self,
        _matcher: &mut dyn ExpressionVisitor,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
    ) -> bool {
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
