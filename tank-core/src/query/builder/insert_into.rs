use crate::{Driver, DynQuery, Entity, NA, SqlWriter};
use std::marker::PhantomData;

pub struct InsertIntoQueryBuilder<E: Entity, Values, Update> {
    pub(crate) values: Values,
    pub(crate) update: bool,
    pub(crate) _table: PhantomData<E>,
    pub(crate) _update: PhantomData<Update>,
}

impl<E: Entity> InsertIntoQueryBuilder<E, NA, NA> {
    pub fn values<'a, Values>(self, values: Values) -> InsertIntoQueryBuilder<E, Values, NA>
    where
        E: 'a,
        Values: IntoIterator<Item = &'a E>,
    {
        InsertIntoQueryBuilder {
            values,
            update: false,
            _table: Default::default(),
            _update: Default::default(),
        }
    }
}

impl<'a, E, V, U> InsertIntoQueryBuilder<E, V, U>
where
    E: Entity + 'a,
    V: IntoIterator<Item = &'a E> + Clone,
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
        writer.write_insert::<E>(&mut query, self.values.clone(), self.update);
        query.into()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_create_table::<E>(out, self.update);
    }
}
