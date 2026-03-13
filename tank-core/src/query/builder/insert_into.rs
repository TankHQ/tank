use crate::{Driver, DynQuery, Entity, NA, SqlWriter};
use std::marker::PhantomData;

pub trait InsertIntoQuery<'a, E: Entity + 'a> {
    fn into_values(self) -> impl IntoIterator<Item = &'a E>;
    fn get_update(&self) -> bool;
}

pub struct InsertIntoQueryBuilder<E: Entity, Values> {
    pub(crate) values: Values,
    pub(crate) update: bool,
    pub(crate) _table: PhantomData<E>,
}

impl<E: Entity> InsertIntoQueryBuilder<E, NA> {
    pub fn values<'a, Values>(self, values: Values) -> InsertIntoQueryBuilder<E, Values>
    where
        E: 'a,
        Values: IntoIterator<Item = &'a E>,
    {
        InsertIntoQueryBuilder {
            values,
            update: false,
            _table: Default::default(),
        }
    }
}

impl<'a, E: Entity + 'a, V> InsertIntoQueryBuilder<E, V> {
    pub fn update(self) -> InsertIntoQueryBuilder<E, V> {
        InsertIntoQueryBuilder {
            values: self.values,
            update: true,
            _table: Default::default(),
        }
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery)
    where
        E: 'a,
        V: IntoIterator<Item = &'a E>,
    {
        let writer = driver.sql_writer();
        writer.write_insert(out, self);
    }
}

impl<'a, E, V> InsertIntoQuery<'a, E> for InsertIntoQueryBuilder<E, V>
where
    E: Entity + 'a,
    V: IntoIterator<Item = &'a E>,
{
    fn into_values(self) -> impl IntoIterator<Item = &'a E> {
        self.values
    }

    fn get_update(&self) -> bool {
        self.update
    }
}

impl<'a, E, I> InsertIntoQuery<'a, E> for I
where
    E: Entity + 'a,
    I: IntoIterator<Item = &'a E>,
{
    fn into_values(self) -> impl IntoIterator<Item = &'a E> {
        self
    }

    fn get_update(&self) -> bool {
        false
    }
}
