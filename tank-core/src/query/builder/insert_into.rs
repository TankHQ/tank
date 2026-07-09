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
        Values: IntoIterator,
        Values::Item: AsEntity,
    {
        InsertIntoQueryBuilder {
            values,
            update: false,
            _update: Default::default(),
        }
    }
}

impl<V, U> InsertIntoQueryBuilder<V, U>
where
    V: IntoIterator + Clone,
    V::Item: AsEntity,
{
    pub fn get_values(&self) -> V {
        self.values.clone()
    }

    pub fn get_update(&self) -> bool {
        self.update
    }

    pub fn build<D: Driver>(&self, driver: &D) -> String {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_insert(&mut query, self.values.clone(), self.update);
        query.into()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_insert(out, self.values.clone(), self.update);
    }
}
