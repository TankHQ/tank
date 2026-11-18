use crate::{
    AnyQuery, BoxQueryBuilder, Dataset, Driver, Expression, ExpressionCollection, NA, Query,
    SqlWriter,
};

pub struct UpdateQueryBuilder<From, Set, Where> {
    pub(crate) from: From,
    pub(crate) set: Set,
    pub(crate) where_expr: Option<Where>,
}

impl<F: Dataset> UpdateQueryBuilder<F, (), NA> {
    /// Assigns the SET clause, each assignment written as an `=` expression.
    ///
    /// Targets render unqualified, as SQL requires: `SET "price" = "price" * 2`.
    pub fn set<Set: ExpressionCollection>(self, set: Set) -> UpdateQueryBuilder<F, Set, NA> {
        UpdateQueryBuilder {
            from: self.from,
            set,
            where_expr: None,
        }
    }
}

impl<F, S> UpdateQueryBuilder<F, S, NA>
where
    F: Dataset,
    S: ExpressionCollection,
{
    pub fn where_expr<Where: Expression>(
        self,
        condition: Where,
    ) -> UpdateQueryBuilder<F, S, Where> {
        UpdateQueryBuilder {
            from: self.from,
            set: self.set,
            where_expr: Some(condition),
        }
    }
}

impl<F, S, W> UpdateQueryBuilder<F, S, W>
where
    F: Dataset,
    S: ExpressionCollection,
    W: Expression,
{
    pub const fn get_from(&self) -> &F {
        &self.from
    }

    pub fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.set.expr_iter()
    }

    pub const fn get_where(&self) -> &Option<W> {
        &self.where_expr
    }
}

/// UPDATE table SET column = value, ... [WHERE condition]
///
/// Use `Sql().update(dataset).set(assignments)` to start.
pub trait UpdateQuery<From: Dataset> {
    /// Get the dataset holding the updated rows.
    fn get_from(&self) -> &From;
    /// Get SET clause assignments.
    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone;
    /// Get WHERE clause expression.
    fn get_where(&self) -> &Option<impl Expression>;
}

impl<F, S, W> UpdateQuery<F> for UpdateQueryBuilder<F, S, W>
where
    F: Dataset,
    S: ExpressionCollection,
    W: Expression,
{
    fn get_from(&self) -> &F {
        self.get_from()
    }

    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.get_set()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        self.get_where()
    }
}

impl<'t, From, T> UpdateQuery<From> for &'t T
where
    From: Dataset + 't,
    T: UpdateQuery<From>,
{
    fn get_from(&self) -> &From {
        (*self).get_from()
    }

    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        (*self).get_set()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        (*self).get_where()
    }
}

impl<F, S, W> UpdateQueryBuilder<F, S, W>
where
    F: Dataset,
    S: ExpressionCollection,
    W: Expression,
{
    /// Builds the query for `driver`.
    pub fn build<D: Driver>(&self, driver: &D) -> Query<D> {
        let mut query = AnyQuery::default();
        self.write_into(&driver.sql_writer(), &mut query);
        query.into()
    }

    /// Writes the statement into an existing query.
    pub fn write_into(&self, writer: &impl SqlWriter, out: &mut AnyQuery) {
        writer.write_update(out, self);
    }

    /// Erases the builder into a [`BoxQueryBuilder`], to store it and run it later on any executor.
    pub fn boxed(self) -> BoxQueryBuilder
    where
        Self: 'static,
    {
        BoxQueryBuilder::new(move |writer, out| self.write_into(writer, out))
    }
}
