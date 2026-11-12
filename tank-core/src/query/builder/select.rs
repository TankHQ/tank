use crate::{
    Dataset, Driver, DynQuery, EitherIterator, Expression, ExpressionCollection, NA, SqlWriter,
};
use std::{iter, marker::PhantomData};

/// SELECT query builder, `Insert` holds the target of an INSERT ... SELECT.
pub struct SelectQueryBuilder<Select, From, Where, GroupBy, Having, OrderBy, Limit, Insert = NA> {
    pub(crate) select: Select,
    pub(crate) from: Option<From>,
    pub(crate) where_expr: Option<Where>,
    pub(crate) group_by: Option<GroupBy>,
    pub(crate) having: Option<Having>,
    pub(crate) order_by: Option<OrderBy>,
    pub(crate) limit: Option<u32>,
    pub(crate) _l: PhantomData<Limit>,
    pub(crate) insert: Insert,
}

impl<S, I> SelectQueryBuilder<S, NA, NA, NA, NA, NA, NA, I> {
    pub fn from<From: Dataset>(
        self,
        from: From,
    ) -> SelectQueryBuilder<S, From, NA, NA, NA, NA, NA, I> {
        SelectQueryBuilder {
            select: self.select,
            from: Some(from),
            where_expr: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, F, I> SelectQueryBuilder<S, F, NA, NA, NA, NA, NA, I> {
    pub fn where_expr<Where>(
        self,
        condition: Where,
    ) -> SelectQueryBuilder<S, F, Where, NA, NA, NA, NA, I>
    where
        Where: Expression,
    {
        SelectQueryBuilder {
            select: self.select,
            from: self.from,
            where_expr: Some(condition),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, F, W, I> SelectQueryBuilder<S, F, W, NA, NA, NA, NA, I> {
    pub fn group_by<GroupBy>(
        self,
        group_by: GroupBy,
    ) -> SelectQueryBuilder<S, F, W, GroupBy, NA, NA, NA, I>
    where
        GroupBy: Clone,
    {
        SelectQueryBuilder {
            select: self.select,
            from: self.from,
            where_expr: self.where_expr,
            group_by: Some(group_by),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, F, W, G, I> SelectQueryBuilder<S, F, W, G, NA, NA, NA, I> {
    pub fn having<Having: Expression>(
        self,
        having: Having,
    ) -> SelectQueryBuilder<S, F, W, G, Having, NA, NA, I> {
        SelectQueryBuilder {
            select: self.select,
            from: self.from,
            where_expr: self.where_expr,
            group_by: self.group_by,
            having: Some(having),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, F, W, G, H, I> SelectQueryBuilder<S, F, W, G, H, NA, NA, I> {
    pub fn order_by<OrderBy>(
        self,
        order_by: OrderBy,
    ) -> SelectQueryBuilder<S, F, W, G, H, OrderBy, NA, I> {
        SelectQueryBuilder {
            select: self.select,
            from: self.from,
            where_expr: self.where_expr,
            group_by: self.group_by,
            having: self.having,
            order_by: Some(order_by),
            limit: None,
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, F, W, G, H, O, I> SelectQueryBuilder<S, F, W, G, H, O, NA, I> {
    pub fn limit(self, limit: Option<u32>) -> SelectQueryBuilder<S, F, W, G, H, O, u32, I> {
        SelectQueryBuilder {
            select: self.select,
            from: self.from,
            where_expr: self.where_expr,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit,
            _l: Default::default(),
            insert: self.insert,
        }
    }
}

impl<S, From, W, G, H, O, L, I> SelectQueryBuilder<S, From, W, G, H, O, L, I>
where
    S: ExpressionCollection,
    From: Dataset,
    W: Expression,
    G: ExpressionCollection,
    H: Expression,
    O: ExpressionCollection,
{
    pub fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.select.expr_iter()
    }

    pub fn get_from(&self) -> &Option<From> {
        &self.from
    }

    pub fn get_where(&self) -> &Option<impl Expression> {
        &self.where_expr
    }

    pub fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        match &self.group_by {
            Some(v) => EitherIterator::Left(v.expr_iter()),
            None => EitherIterator::Right(iter::empty()),
        }
    }

    pub fn get_having(&self) -> &Option<impl Expression> {
        &self.having
    }

    pub fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        match &self.order_by {
            Some(v) => EitherIterator::Left(v.expr_iter()),
            None => EitherIterator::Right(iter::empty()),
        }
    }

    pub fn get_limit(&self) -> Option<u32> {
        self.limit
    }
}

impl<S, From, W, G, H, O, L> SelectQueryBuilder<S, From, W, G, H, O, L>
where
    S: ExpressionCollection,
    From: Dataset,
    W: Expression,
    G: ExpressionCollection,
    H: Expression,
    O: ExpressionCollection,
{
    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        SelectQuery::build(self, driver)
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        SelectQuery::build_into(self, driver, out);
    }
}

/// SELECT query builder.
///
/// Use `Sql.select(columns)` to start.
pub trait SelectQuery<From>
where
    From: Dataset,
{
    /// Get SELECT expressions.
    fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone;
    /// Get FROM clause dataset.
    fn get_from<'s>(&'s self) -> &'s Option<From>;
    /// Get WHERE clause expression.
    fn get_where<'s>(&'s self) -> &'s Option<impl Expression>;
    /// Get GROUP BY expressions.
    fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone;
    /// Get HAVING clause expression.
    fn get_having(&self) -> &Option<impl Expression>;
    /// Get ORDER BY expressions.
    fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone;
    /// Get LIMIT value.
    fn get_limit(&self) -> Option<u32>;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<S, From, W, G, H, O, L, I> SelectQuery<From> for SelectQueryBuilder<S, From, W, G, H, O, L, I>
where
    S: ExpressionCollection,
    From: Dataset,
    W: Expression,
    G: ExpressionCollection,
    H: Expression,
    O: ExpressionCollection,
{
    fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.get_select()
    }

    fn get_from(&self) -> &Option<From> {
        self.get_from()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        self.get_where()
    }

    fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.get_group_by()
    }

    fn get_having(&self) -> &Option<impl Expression> {
        self.get_having()
    }

    fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.get_order_by()
    }

    fn get_limit(&self) -> Option<u32> {
        self.get_limit()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(driver, &mut query);
        query
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        driver.sql_writer().write_select(out, self);
    }
}

impl<'t, From: Dataset + 't, T: SelectQuery<From>> SelectQuery<From> for &'t T {
    fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
        (**self).get_select()
    }

    fn get_from(&self) -> &Option<From> {
        (**self).get_from()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        (**self).get_where()
    }

    fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        (**self).get_group_by()
    }

    fn get_having(&self) -> &Option<impl Expression> {
        (**self).get_having()
    }

    fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        (**self).get_order_by()
    }

    fn get_limit(&self) -> Option<u32> {
        (**self).get_limit()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        (**self).build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        (**self).build_into(driver, out);
    }
}
