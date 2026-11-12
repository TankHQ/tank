use crate::{
    AsValue, ColumnDef, Dataset, Driver, DynQuery, Entity, Expression, ExpressionCollection, NA,
    SelectQuery, SelectQueryBuilder, SqlWriter, TableRef, Value, future::Either,
};
use std::{borrow::Cow, iter, marker::PhantomData};

type NoSelect = SelectQueryBuilder<NA, NA, NA, NA, NA, NA, NA>;

/// INSERT ... SELECT, a SELECT carrying the table and columns its rows are inserted into.
pub type InsertSelectQueryBuilder<S, F = NA, W = NA, G = NA, H = NA, O = NA, L = NA> =
    SelectQueryBuilder<S, F, W, G, H, O, L, InsertIntoQueryBuilder<NA, NA>>;

/// INSERT query builder, inserting rows of values or the result of a SELECT.
pub struct InsertIntoQueryBuilder<Values, Update> {
    pub(crate) table: TableRef,
    pub(crate) columns: Cow<'static, [ColumnDef]>,
    pub(crate) values: Values,
    pub(crate) update: bool,
    pub(crate) _update: PhantomData<Update>,
}

impl<V, U> InsertIntoQueryBuilder<V, U> {
    fn with<Values, Update>(
        self,
        values: Values,
        update: bool,
    ) -> InsertIntoQueryBuilder<Values, Update> {
        InsertIntoQueryBuilder {
            table: self.table,
            columns: self.columns,
            values,
            update,
            _update: PhantomData,
        }
    }
}

impl InsertIntoQueryBuilder<NA, NA> {
    /// Insert entities, the columns and their values.
    pub fn values<'a, E, Entities>(
        mut self,
        entities: Entities,
    ) -> InsertIntoQueryBuilder<impl Iterator<Item = impl Iterator<Item = Value<'a>>> + Clone, NA>
    where
        E: Entity + 'a,
        Entities: IntoIterator<Item = &'a E>,
        Entities::IntoIter: Clone,
    {
        // A table named by hand takes the columns and primary key of the entities.
        if self.table.columns.is_empty() {
            self.table.columns = E::columns();
            self.table.primary_key = E::table().primary_key;
        }
        self.columns = Cow::Borrowed(E::columns());
        self.with(entities.into_iter().map(E::row_values), false)
    }

    /// Name the columns of the values or of the SELECT that follow.
    pub fn columns<Name: Into<Cow<'static, str>>>(
        mut self,
        columns: impl IntoIterator<Item = Name>,
    ) -> InsertIntoQueryBuilder<(), NA> {
        self.columns = Cow::Owned(columns.into_iter().map(ColumnDef::new).collect());
        self.with((), false)
    }

    /// Insert the rows of a SELECT, continued like any SELECT, matched to the table columns by position.
    pub fn select<S: ExpressionCollection>(self, select: S) -> InsertSelectQueryBuilder<S> {
        self.with((), false).select(select)
    }
}

impl InsertIntoQueryBuilder<(), NA> {
    /// Insert rows of values in the order of the columns, such as `[[1, 2], [3, 4]]` or vectors of [`Value`].
    pub fn values<Rows>(self, rows: Rows) -> InsertIntoQueryBuilder<Rows, NA>
    where
        Rows: IntoIterator + Clone,
        Rows::Item: IntoIterator,
        <Rows::Item as IntoIterator>::Item: AsValue,
    {
        self.with(rows, false)
    }

    /// Insert the rows of a SELECT, continued like any SELECT, into the columns.
    pub fn select<S: ExpressionCollection>(self, select: S) -> InsertSelectQueryBuilder<S> {
        SelectQueryBuilder {
            select,
            from: None,
            where_expr: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            _l: PhantomData,
            insert: self.with(NA, false),
        }
    }
}

impl<R> InsertIntoQueryBuilder<R, NA>
where
    R: IntoIterator + Clone,
    R::Item: IntoIterator,
    <R::Item as IntoIterator>::Item: AsValue,
{
    /// Update the existing rows on a conflict on the table primary key (upsert).
    pub fn update(self) -> InsertIntoQueryBuilder<R, bool> {
        InsertIntoQueryBuilder {
            table: self.table,
            columns: self.columns,
            values: self.values,
            update: true,
            _update: PhantomData,
        }
    }
}

impl<V, U> InsertIntoQueryBuilder<V, U>
where
    Self: InsertIntoQuery,
{
    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        InsertIntoQuery::build(self, driver)
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        InsertIntoQuery::build_into(self, driver, out);
    }
}

impl<R, U> InsertIntoQuery for InsertIntoQueryBuilder<R, U>
where
    R: IntoIterator + Clone,
    R::Item: IntoIterator,
    <R::Item as IntoIterator>::Item: AsValue,
{
    type From = NA;

    fn get_table(&self) -> &TableRef {
        &self.table
    }

    fn get_columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    fn get_values(
        &self,
    ) -> Either<impl Iterator<Item = impl Iterator<Item = Value<'_>>>, &impl SelectQuery<Self::From>>
    {
        Either::<_, &NoSelect>::Left(
            self.values
                .clone()
                .into_iter()
                .map(|row| row.into_iter().map(AsValue::as_value)),
        )
    }

    fn get_update(&self) -> bool {
        self.update
    }
}

impl<S, F, W, G, H, O, L> InsertSelectQueryBuilder<S, F, W, G, H, O, L>
where
    S: ExpressionCollection,
    F: Dataset,
    W: Expression,
    G: ExpressionCollection,
    H: Expression,
    O: ExpressionCollection,
{
    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        InsertIntoQuery::build(self, driver)
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        InsertIntoQuery::build_into(self, driver, out);
    }
}

impl<S, F, W, G, H, O, L> InsertIntoQuery for InsertSelectQueryBuilder<S, F, W, G, H, O, L>
where
    S: ExpressionCollection,
    F: Dataset,
    W: Expression,
    G: ExpressionCollection,
    H: Expression,
    O: ExpressionCollection,
{
    type From = F;

    fn get_table(&self) -> &TableRef {
        &self.insert.table
    }

    fn get_columns(&self) -> &[ColumnDef] {
        &self.insert.columns
    }

    fn get_values(
        &self,
    ) -> Either<impl Iterator<Item = impl Iterator<Item = Value<'_>>>, &impl SelectQuery<Self::From>>
    {
        Either::Right::<iter::Empty<iter::Empty<Value>>, _>(self)
    }

    fn get_update(&self) -> bool {
        false
    }
}

/// INSERT query builder.
///
/// Use `Sql().insert_into(table)` to start, then `values(&entities)`, `columns(names).values(rows)`
/// or `select(columns).from(dataset)...`, failing on primary key conflicts without `update`.
pub trait InsertIntoQuery {
    /// Dataset of the SELECT producing the rows, unused for rows of values.
    type From: Dataset;
    /// Get the table the rows are inserted into.
    fn get_table(&self) -> &TableRef;
    /// Get the columns, empty for all the table columns in order.
    fn get_columns(&self) -> &[ColumnDef];
    /// Get the rows of values, each in the order of the columns, or the SELECT producing them.
    fn get_values(
        &self,
    ) -> Either<impl Iterator<Item = impl Iterator<Item = Value<'_>>>, &impl SelectQuery<Self::From>>;
    /// Whether existing rows are updated on a primary key conflict.
    fn get_update(&self) -> bool;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery
    where
        Self: Sized,
    {
        let mut query = DynQuery::default();
        self.build_into(driver, &mut query);
        query
    }
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery)
    where
        Self: Sized,
    {
        driver.sql_writer().write_insert(out, self);
    }
}

impl<T: InsertIntoQuery> InsertIntoQuery for &T {
    type From = T::From;

    fn get_table(&self) -> &TableRef {
        (**self).get_table()
    }

    fn get_columns(&self) -> &[ColumnDef] {
        (**self).get_columns()
    }

    fn get_values(
        &self,
    ) -> Either<impl Iterator<Item = impl Iterator<Item = Value<'_>>>, &impl SelectQuery<Self::From>>
    {
        (**self).get_values()
    }

    fn get_update(&self) -> bool {
        (**self).get_update()
    }
}
