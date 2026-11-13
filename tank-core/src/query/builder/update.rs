use crate::{Dataset, Driver, DynQuery, Expression, ExpressionCollection, NA, SqlWriter};

/// UPDATE query builder.
///
/// Use `Sql().update(dataset).set(assignments)` to start.
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

impl<F: Dataset, S: ExpressionCollection> UpdateQueryBuilder<F, S, NA> {
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

impl<F: Dataset, S: ExpressionCollection, W: Expression> UpdateQueryBuilder<F, S, W> {
    pub fn get_from(&self) -> &F {
        &self.from
    }

    pub fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.set.expr_iter()
    }

    pub fn get_where(&self) -> &Option<W> {
        &self.where_expr
    }

    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(driver, &mut query);
        query
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        driver.sql_writer().write_update(out, self);
    }
}

/// UPDATE query statement.
///
/// Build one with `Sql().update(dataset).set(cols!(E::field == value = E::field + 1, ...))`.
pub trait UpdateQuery<From: Dataset> {
    /// Get the dataset holding the updated rows.
    fn get_from(&self) -> &From;
    /// Get SET clause assignments.
    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone;
    /// Get WHERE clause expression.
    fn get_where(&self) -> &Option<impl Expression>;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<F: Dataset, S: ExpressionCollection, W: Expression> UpdateQuery<F>
    for UpdateQueryBuilder<F, S, W>
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

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        self.build_into(driver, out);
    }
}

impl<'t, From: Dataset + 't, T: UpdateQuery<From>> UpdateQuery<From> for &'t T {
    fn get_from(&self) -> &From {
        (*self).get_from()
    }

    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        (*self).get_set()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        (*self).get_where()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        (*self).build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        (*self).build_into(driver, out);
    }
}
