use crate::{AnyQuery, BoxQueryBuilder, Dataset, Driver, Expression, NA, Query, SqlWriter};

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

impl<F, W> DeleteQueryBuilder<F, W>
where
    F: Dataset,
    W: Expression,
{
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

impl<F, W> DeleteQuery<F> for DeleteQueryBuilder<F, W>
where
    F: Dataset,
    W: Expression,
{
    fn get_from(&self) -> &F {
        self.get_from()
    }

    fn get_where(&self) -> &Option<impl Expression> {
        self.get_where()
    }
}

impl<F, W> DeleteQueryBuilder<F, W>
where
    F: Dataset,
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
        match &self.where_expr {
            Some(condition) => writer.write_delete(out, &self.from, condition),
            None => writer.write_delete(out, &self.from, true),
        }
    }

    /// Erases the builder into a [`BoxQueryBuilder`], to store it and run it later on any executor.
    pub fn boxed(self) -> BoxQueryBuilder
    where
        Self: 'static,
    {
        BoxQueryBuilder::new(move |writer, out| self.write_into(writer, out))
    }
}
