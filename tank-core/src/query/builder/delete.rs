use crate::{AnyQuery, Dataset, Driver, Expression, NA, QueryBuilder, SqlWriter};

pub struct DeleteQueryBuilder<From, Where> {
    pub(crate) from: From,
    pub(crate) where_expr: Option<Where>,
}

impl<F: Dataset> DeleteQueryBuilder<F, NA> {
    pub fn where_expr<Where: Expression>(self, condition: Where) -> DeleteQueryBuilder<F, Where> {
        DeleteQueryBuilder {
            from: self.from,
            where_expr: Some(condition),
        }
    }
}

impl<F: Dataset, W: Expression> DeleteQueryBuilder<F, W> {
    pub const fn get_from(&self) -> &F {
        &self.from
    }

    pub const fn get_where(&self) -> &Option<W> {
        &self.where_expr
    }
}

/// DELETE FROM table [WHERE condition]
///
/// Use `Sql().delete_from(dataset)` to start, deleting every row without `where_expr`.
pub trait DeleteQuery<From: Dataset> {
    /// Get the dataset rows are deleted from.
    fn get_from(&self) -> &From;
    /// Get WHERE clause expression.
    fn get_where(&self) -> &Option<impl Expression>;
}

impl<F: Dataset, W: Expression> DeleteQuery<F> for DeleteQueryBuilder<F, W> {
    fn get_from(&self) -> &F {
        self.get_from()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        self.get_where()
    }
}

impl<F: Dataset, W: Expression> QueryBuilder for DeleteQueryBuilder<F, W> {
    fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery) {
        let writer = driver.sql_writer();
        match &self.where_expr {
            Some(condition) => writer.write_delete(out, &self.from, condition),
            None => writer.write_delete(out, &self.from, true),
        }
    }
}
