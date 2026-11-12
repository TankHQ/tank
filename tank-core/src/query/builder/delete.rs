use crate::{Dataset, Driver, DynQuery, Expression, NA, SqlWriter};

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
    pub fn get_from(&self) -> &F {
        &self.from
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
        let writer = driver.sql_writer();
        match &self.where_expr {
            Some(condition) => writer.write_delete(out, &self.from, condition),
            None => writer.write_delete(out, &self.from, true),
        }
    }
}

/// DELETE query builder.
///
/// Use `Sql().delete_from(dataset)` to start, deleting every row without `where_expr`.
pub trait DeleteQuery<From: Dataset> {
    /// Get the dataset rows are deleted from.
    fn get_from(&self) -> &From;
    /// Get WHERE clause expression.
    fn get_where(&self) -> &Option<impl Expression>;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<F: Dataset, W: Expression> DeleteQuery<F> for DeleteQueryBuilder<F, W> {
    fn get_from(&self) -> &F {
        self.get_from()
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
