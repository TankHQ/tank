use crate::{
    AnyQuery, Dataset, Driver, Expression, ExpressionCollection, NA, QueryBuilder, SqlWriter,
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
}

impl<F: Dataset, S: ExpressionCollection, W: Expression> QueryBuilder
    for UpdateQueryBuilder<F, S, W>
{
    fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery) {
        driver.sql_writer().write_update(out, self);
    }
}
