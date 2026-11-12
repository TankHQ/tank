mod create_table;
mod delete;
mod drop_table;
mod insert_into;
mod select;

pub use create_table::*;
pub use delete::*;
pub use drop_table::*;
pub use insert_into::*;
pub use select::*;

use crate::{
    Context, Dataset, DynQuery, Expression, ExpressionVisitor, OpPrecedence, SqlWriter, TableRef,
};
use std::{borrow::Cow, iter};

/// Entry point of the query builders, as in `Sql.select(columns).from(table)`.
#[derive(Default, Debug)]
pub struct Sql;

impl Sql {
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
            insert: NA,
        }
    }
    pub fn insert_into(self, table: impl Into<TableRef>) -> InsertIntoQueryBuilder<NA, NA> {
        InsertIntoQueryBuilder {
            table: table.into(),
            columns: Cow::Borrowed(&[]),
            values: NA,
            update: false,
            _update: Default::default(),
        }
    }
    pub fn delete_from<From: Dataset>(self, from: From) -> DeleteQueryBuilder<From, NA> {
        DeleteQueryBuilder {
            from,
            where_expr: Default::default(),
        }
    }
    pub fn create_table(self, table: impl Into<TableRef>) -> CreateTableQueryBuilder<NA> {
        CreateTableQueryBuilder {
            table: table.into(),
            if_not_exists: Default::default(),
            _e: Default::default(),
        }
    }
    pub fn drop_table(self, table: impl Into<TableRef>) -> DropTableQueryBuilder<NA> {
        DropTableQueryBuilder {
            table: table.into(),
            if_exists: Default::default(),
            _e: Default::default(),
        }
    }
}

#[derive(Default, Debug)]
pub struct NA;

impl Dataset for NA {
    fn qualified_columns(&self) -> bool {
        false
    }
    fn write_table_name(
        &self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
    ) {
    }
    fn table_ref(&self) -> TableRef {
        TableRef::default()
    }
}

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
