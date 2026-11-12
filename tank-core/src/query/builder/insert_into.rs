use crate::{AsEntity, Driver, DynQuery, NA, SqlWriter};
use std::marker::PhantomData;

pub struct InsertIntoQueryBuilder<Values, Update> {
    pub(crate) values: Values,
    pub(crate) update: bool,
    pub(crate) _update: PhantomData<Update>,
}

impl InsertIntoQueryBuilder<NA, NA> {
    pub fn values<Values>(self, values: Values) -> InsertIntoQueryBuilder<Values, NA>
    where
        Values: IntoIterator + Send,
        Values::IntoIter: Send,
        Values::Item: AsEntity,
    {
        InsertIntoQueryBuilder {
            values,
            update: false,
            _update: Default::default(),
        }
    }
}

impl<V> InsertIntoQueryBuilder<V, NA> {
    /// Update the existing rows on a primary key conflict (upsert).
    pub fn update(self) -> InsertIntoQueryBuilder<V, bool> {
        InsertIntoQueryBuilder {
            values: self.values,
            update: true,
            _update: Default::default(),
        }
    }
}

impl<V, U> InsertIntoQueryBuilder<V, U>
where
    V: IntoIterator + Clone + Send,
    V::IntoIter: Send,
    V::Item: AsEntity,
{
    pub fn get_values(&self) -> V {
        self.values.clone()
    }

    pub fn get_update(&self) -> bool {
        self.update
    }

    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_insert(&mut query, self.values.clone(), self.update);
        query
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_insert(out, self.values.clone(), self.update);
    }
}

/// INSERT query builder.
///
/// Use `QueryBuilder::new().insert_into().values(entities)` to start, failing on primary key conflicts without `update`.
pub trait InsertIntoQuery<Values>
where
    Values: IntoIterator + Clone + Send,
    Values::IntoIter: Send,
    Values::Item: AsEntity,
{
    /// Get the entities to insert.
    fn get_values(&self) -> Values;
    /// Whether existing rows are updated on a primary key conflict.
    fn get_update(&self) -> bool;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<V, U> InsertIntoQuery<V> for InsertIntoQueryBuilder<V, U>
where
    V: IntoIterator + Clone + Send,
    V::IntoIter: Send,
    V::Item: AsEntity,
{
    fn get_values(&self) -> V {
        self.get_values()
    }

    fn get_update(&self) -> bool {
        self.get_update()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        self.build_into(driver, out);
    }
}
