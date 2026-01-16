use crate::{
    Context, DataSet, Driver, DynQuery, EitherIterator, Expression, OpPrecedence, SqlWriter,
};
use std::{iter, marker::PhantomData};

#[derive(Debug)]
pub struct NA;

impl OpPrecedence for NA {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        0
    }
}

impl Expression for NA {
    fn write_query(&self, _writer: &dyn SqlWriter, _context: &mut Context, _out: &mut DynQuery) {}
}

pub struct Builder<Select, From, Where, GroupBy, Having, OrderBy, Limit> {
    pub(crate) select: Select,
    pub(crate) from: Option<From>,
    pub(crate) where_condition: Option<Where>,
    pub(crate) group_by: Option<GroupBy>,
    pub(crate) having: Option<Having>,
    pub(crate) order_by: Option<OrderBy>,
    pub(crate) limit: Option<u32>,
    pub(crate) _l: PhantomData<Limit>,
}

pub type QueryBuilder = Builder<NA, NA, NA, NA, NA, NA, NA>;

impl Builder<NA, NA, NA, NA, NA, NA, NA> {
    pub fn new() -> Self {
        Self {
            select: NA,
            from: Default::default(),
            where_condition: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
    pub fn select<Select>(self, select: Select) -> Builder<Select, NA, NA, NA, NA, NA, NA> {
        Builder {
            select,
            from: Default::default(),
            where_condition: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
}

impl<S> Builder<S, NA, NA, NA, NA, NA, NA> {
    pub fn from<From: DataSet>(self, from: From) -> Builder<S, From, NA, NA, NA, NA, NA> {
        Builder {
            select: self.select,
            from: Some(from),
            where_condition: Default::default(),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
}

impl<S, F> Builder<S, F, NA, NA, NA, NA, NA> {
    pub fn where_condition<Where>(self, condition: Where) -> Builder<S, F, Where, NA, NA, NA, NA>
    where
        Where: Expression,
    {
        Builder {
            select: self.select,
            from: self.from,
            where_condition: Some(condition),
            group_by: Default::default(),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
}

impl<S, F, W> Builder<S, F, W, NA, NA, NA, NA> {
    pub fn group_by<GroupBy>(self, group_by: GroupBy) -> Builder<S, F, W, GroupBy, NA, NA, NA>
    where
        GroupBy: Clone,
    {
        Builder {
            select: self.select,
            from: self.from,
            where_condition: self.where_condition,
            group_by: Some(group_by),
            having: Default::default(),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
}

impl<S, F, W, G> Builder<S, F, W, G, NA, NA, NA> {
    pub fn having<Having: Expression>(self, having: Having) -> Builder<S, F, W, G, Having, NA, NA> {
        Builder {
            select: self.select,
            from: self.from,
            where_condition: self.where_condition,
            group_by: self.group_by,
            having: Some(having),
            order_by: Default::default(),
            limit: Default::default(),
            _l: Default::default(),
        }
    }
}

impl<S, F, W, G, H> Builder<S, F, W, G, H, NA, NA> {
    pub fn order_by<OrderBy>(self, order_by: OrderBy) -> Builder<S, F, W, G, H, OrderBy, u32> {
        Builder {
            select: self.select,
            from: self.from,
            where_condition: self.where_condition,
            group_by: self.group_by,
            having: self.having,
            order_by: Some(order_by),
            limit: None,
            _l: Default::default(),
        }
    }
}

impl<S, F, W, G, H, O> Builder<S, F, W, G, H, O, NA> {
    pub fn limit(self, limit: Option<u32>) -> Builder<S, F, W, G, H, O, u32> {
        Builder {
            select: self.select,
            from: self.from,
            where_condition: self.where_condition,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit,
            _l: Default::default(),
        }
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

impl<S, From, W, G, H, O, L> Builder<S, From, W, G, H, O, L>
where
    S: ExpressionCollection,
    From: DataSet,
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

    pub fn get_where_condition(&self) -> &Option<impl Expression> {
        &self.where_condition
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

    pub fn build<D: Driver>(&self, driver: &D) -> String {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_select(&mut query, self);
        query.into_buffer()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_select(out, self);
    }
}

pub trait QueryData<From>
where
    // Self: 's,
    From: DataSet,
{
    fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone;
    fn get_from<'s>(&'s self) -> &'s Option<From>;
    fn get_where_condition<'s>(&'s self) -> &'s Option<impl Expression>;
    fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone;
    fn get_having(&self) -> &Option<impl Expression>;
    fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone;
    fn get_limit(&self) -> Option<u32>;
    fn build<D: Driver>(&self, driver: &D) -> String;
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<S, From, W, G, H, O, L> QueryData<From> for Builder<S, From, W, G, H, O, L>
where
    S: ExpressionCollection,
    From: DataSet,
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

    fn get_where_condition(&self) -> &Option<impl Expression> {
        self.get_where_condition()
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

    fn build<D: Driver>(&self, driver: &D) -> String {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        self.build_into(driver, out);
    }
}
